mod command;
mod resp;
mod storage;

use {
    command::{Command, CommandError, normalize_command_args, parse},
    resp::RespParser,
};

use {
    bytes::Bytes,
    futures::{SinkExt, StreamExt},
    std::net::SocketAddr,
    tokio::net::{TcpListener, TcpStream},
    tokio_util::codec::Decoder,
};

#[tokio::main]
async fn main() {
    let socket_address = "127.0.0.1:6379";

    let listener = TcpListener::bind(socket_address)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {socket_address}: {e}"));

    loop {
        match listener.accept().await {
            Ok((stream, address)) => {
                tokio::spawn(handle_connection(stream, address));
            }
            Err(e) => {
                println!("error: {e}");
            }
        }
    }
}

async fn handle_connection(stream: TcpStream, _address: SocketAddr) -> Result<(), CommandError> {
    let mut transport = RespParser::default().framed(stream);

    while let Some(raw_command) = transport.next().await {
        println!("raw_command: {raw_command:?}");
        let redis_value = match raw_command {
            Ok(raw_command) => raw_command,
            Err(e) => {
                println!("error: {e}");
                continue;
            }
        };

        let result = match normalize_command_args(&redis_value) {
            Err(e) => Some(RedisValue::Error(e.to_string())),
            Ok(args) => match parse(args.as_slice()) {
                Err(e) => Some(RedisValue::Error(e.to_string())),
                Ok(command) => match command {
                    Command::Ping => Some(RedisValue::SimpleString(Bytes::from("PONG"))),
                    Command::Echo(arg) => Some(RedisValue::BulkString(arg)),
                    Command::Set(operation) => {
                        storage::set(operation);
                        Some(RedisValue::SimpleString(Bytes::from("OK")))
                    }
                    Command::Push(operation) => {
                        let len = storage::push(operation);
                        Some(RedisValue::Integer(len as i64))
                    }
                    Command::Get(key) => storage::get(key)
                        .map(RedisValue::BulkString)
                        .or(Some(RedisValue::Null)),
                    Command::Lrange(operation) => match storage::get_range(operation) {
                        Ok(elements) => Some(RedisValue::Array(
                            elements.into_iter().map(RedisValue::BulkString).collect(),
                        )),
                        Err(e) => Some(RedisValue::Error(e.to_string())),
                    },
                    Command::Llen(key) => Some(RedisValue::Integer(storage::llen(key) as i64)),
                    Command::NoOp => None,
                },
            },
        };

        let Some(result) = result else { continue };

        if let Err(e) = transport.send(result).await {
            println!("error when sending response: {e}");
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum RedisValue {
    BulkString(Bytes),
    SimpleString(Bytes),
    Integer(i64),
    Array(Vec<RedisValue>),
    Error(String),
    Null,
}
