mod resp;
mod storage;

use {
    resp::RespParser,
    storage::{PushOperation, RangeOperation, SetOperation},
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
            Ok(args) => match Command::try_from(args.as_slice()) {
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
                            elements
                                .into_iter()
                                .map(RedisValue::BulkString)
                                .collect(),
                        )),
                        Err(e) => Some(RedisValue::Error(e.to_string())),
                    },
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

enum Command {
    Ping,
    Echo(Bytes),
    Set(SetOperation),
    Push(PushOperation),
    Get(Bytes),
    Lrange(RangeOperation),
    NoOp,
}

#[derive(Debug, thiserror::Error)]
enum CommandError {
    #[error("invalid command: {0}")]
    InvalidCommand(&'static str),
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Turn a decoded top-level RESP value into command argument bytes (one entry per bulk string /
/// inline token): RESP arrays coerce string elements to bytes; inline commands are tokenized by ASCII
/// whitespace.
fn normalize_command_args(value: &RedisValue) -> Result<Vec<Bytes>, CommandError> {
    match value {
        RedisValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RedisValue::BulkString(b) => out.push(b.clone()),
                    RedisValue::SimpleString(s) => out.push(s.clone()),
                    _ => {
                        return Err(CommandError::InvalidArgument(
                            "command arguments must be strings",
                        ));
                    }
                }
            }
            Ok(out)
        }
        RedisValue::SimpleString(line) => {
            let line = line.as_ref();
            let mut out = Vec::new();
            let mut token_start = None::<usize>;
            for (i, &b) in line.iter().enumerate() {
                if b.is_ascii_whitespace() {
                    if let Some(s) = token_start {
                        out.push(Bytes::copy_from_slice(&line[s..i]));
                        token_start = None;
                    }
                } else if token_start.is_none() {
                    token_start = Some(i);
                }
            }
            if let Some(s) = token_start {
                out.push(Bytes::copy_from_slice(&line[s..]));
            }
            Ok(out)
        }
        _ => Err(CommandError::InvalidCommand(
            "expected array or inline command",
        )),
    }
}

impl TryFrom<&[Bytes]> for Command {
    type Error = CommandError;

    fn try_from(value: &[Bytes]) -> Result<Self, Self::Error> {
        if value.len() == 0 {
            return Ok(Command::NoOp);
        }

        let first_bytes = value[0].as_ref();

        let command = String::from_utf8_lossy(first_bytes);
        println!("command: {command}");
        match command.as_ref() {
            "PING" => Ok(Command::Ping),
            "ECHO" => {
                if value.len() != 2 {
                    return Err(CommandError::InvalidArgument(
                        "ECHO command requires a single argument",
                    ));
                }
                if !str::from_utf8(&value[1]).is_ok() {
                    return Err(CommandError::InvalidArgument(
                        "ECHO command requires a valid UTF-8 string argument",
                    ));
                }
                Ok(Command::Echo(value[1].clone()))
            }
            "SET" => {
                let operation = SetOperation::try_from_args(&value[1..])?;
                Ok(Command::Set(operation))
            }
            "RPUSH" => {
                let operation = PushOperation::try_from_args(&value[1..])?;
                Ok(Command::Push(operation))
            }
            "GET" => {
                if value.len() != 2 {
                    return Err(CommandError::InvalidArgument(
                        "GET command requires a single argument",
                    ));
                }
                Ok(Command::Get(value[1].clone()))
            }
            "LRANGE" => {
                let operation = RangeOperation::try_from_args(&value[1..])?;
                Ok(Command::Lrange(operation))
            }
            _ => Err(CommandError::InvalidCommand("Unknown command")),
        }
    }
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
