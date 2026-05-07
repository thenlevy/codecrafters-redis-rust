use {
    std::{io, net::SocketAddr},
    tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    },
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

async fn handle_connection(stream: TcpStream, address: SocketAddr) -> Result<(), io::Error> {
    let (input, mut output) = stream.into_split();
    let mut lines = BufReader::new(input).lines();

    while let Some(cmd) = lines.next_line().await? {
        match Command::from_line(cmd.as_str()) {
            Command::Ping => {
                output.write_all(b"+PONG\r\n").await?;
            }
            Command::Echo(args) => {
                output.write_all(args.as_bytes()).await?;
            }
            Command::Empty => {
                println!("empty command from address {address}");
            }
            Command::Unknown(cmd) => {
                println!("unexpected command from address {address}: {cmd}");
            }
        }
    }

    Ok(())
}

impl<'l> Command<'l> {
    fn from_line(line: &'l str) -> Self {
        let mut words = line.trim().split_whitespace();

        let Some(verb) = words.next() else {
            return Command::Empty;
        };

        match verb {
            "PING" => Command::Ping,
            "ECHO" => {
                let Some((_echo, arg)) = line.trim().split_once(' ') else {
                    return Command::Echo("");
                };
                Command::Echo(arg)
            }
            _ => Command::Unknown(line),
        }
    }
}

enum Command<'l> {
    Ping,
    Echo(&'l str),
    Unknown(&'l str),
    Empty,
}
