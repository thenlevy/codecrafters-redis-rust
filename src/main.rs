mod resp;

use resp::RespParser;

use {
    bytes::Bytes,
    futures::{SinkExt, StreamExt},
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, LazyLock, Mutex},
    },
    thiserror::Error,
    tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    },
    tokio_util::codec::Decoder,
};

static STORAGE: LazyLock<Arc<Mutex<HashMap<Bytes, Bytes>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

#[tokio::main]
async fn main() {
    let socket_address = "127.0.0.1:6379";

    LazyLock::force(&STORAGE);

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

async fn handle_connection(stream: TcpStream, address: SocketAddr) -> Result<(), CommandError> {
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
        let values = match &redis_value {
            RedisValue::Array(values) => values.as_slice(),
            RedisValue::SimpleString(s) => &[RedisValue::SimpleString(Bytes::clone(s))],
            _ => continue,
        };

        let result = match Command::try_from(values) {
            Err(e) => Some(RedisValue::Error(e.to_string())),
            Ok(command) => match command {
                Command::Ping => Some(RedisValue::SimpleString(Bytes::from("PONG"))),
                Command::Echo(arg) => Some(RedisValue::BulkString(arg)),
                Command::Set(key, value) => {
                    STORAGE.lock().unwrap().insert(key, value);
                    Some(RedisValue::SimpleString(Bytes::from("OK")))
                }
                Command::Get(key) => STORAGE
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(|value| RedisValue::BulkString(Bytes::clone(value)))
                    .or(Some(RedisValue::Null)),
                Command::NoOp => None,
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
    Set(Bytes, Bytes),
    Get(Bytes),
    NoOp,
}

#[derive(Debug, thiserror::Error)]
enum CommandError {
    #[error("internal error: {0}")]
    InternalError(&'static str),
    #[error("invalid command: {0}")]
    InvalidCommand(&'static str),
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

impl TryFrom<&[RedisValue]> for Command {
    type Error = CommandError;

    fn try_from(value: &[RedisValue]) -> Result<Self, Self::Error> {
        if value.len() == 0 {
            return Ok(Command::NoOp);
        }

        let Some(first_bytes) = value[0].try_bytes() else {
            return Err(CommandError::InvalidCommand("Command is not a string"));
        };

        let command = String::from_utf8_lossy(&first_bytes);
        println!("command: {command}");
        match command.as_ref() {
            "PING" => Ok(Command::Ping),
            "ECHO" => {
                if value.len() != 2 {
                    return Err(CommandError::InvalidArgument(
                        "ECHO command requires a single argument",
                    ));
                }
                let Some(arg_bytes) = value[1].try_bytes() else {
                    return Err(CommandError::InvalidArgument(
                        "ECHO command requires a string argument",
                    ));
                };
                if !str::from_utf8(&arg_bytes).is_ok() {
                    return Err(CommandError::InvalidArgument(
                        "ECHO command requires a valid UTF-8 string argument",
                    ));
                }
                Ok(Command::Echo(arg_bytes))
            }
            "SET" => {
                if value.len() != 3 {
                    return Err(CommandError::InvalidArgument(
                        "SET command requires two arguments",
                    ));
                }
                Ok(Command::Set(
                    value[1].try_bytes().unwrap(),
                    value[2].try_bytes().unwrap(),
                ))
            }
            "GET" => {
                if value.len() != 2 {
                    return Err(CommandError::InvalidArgument(
                        "GET command requires a single argument",
                    ));
                }
                Ok(Command::Get(value[1].try_bytes().unwrap()))
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

impl RedisValue {
    fn try_bytes(&self) -> Option<Bytes> {
        match self {
            RedisValue::BulkString(s) | RedisValue::SimpleString(s) => Some(Bytes::clone(s)),
            RedisValue::Error(_)
            | RedisValue::Integer(_)
            | RedisValue::Array(_)
            | RedisValue::Null => None,
        }
    }
}
