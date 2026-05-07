mod resp;

use resp::{Command, CommandError, RespParser};

use {
    std::net::SocketAddr,
    tokio::{
        io::AsyncWriteExt,
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

async fn handle_connection(stream: TcpStream, address: SocketAddr) -> Result<(), CommandError> {
    let (input, mut output) = stream.into_split();
    let mut rest_parser = RespParser::new(input);

    while let Some(raw_command) = rest_parser.next_raw_command().await? {
        match Command::from(&raw_command) {
            Command::Ping => {
                output.write_all(b"+PONG\r\n").await?;
            }
            Command::Echo(arg) => {
                output.write_all(arg.as_bytes()).await?;
            }
            Command::EchoOwned(args) => {
                for (n, arg) in args.iter().enumerate() {
                    if n > 0 {
                        output.write_all(b" ").await?;
                    }
                    output.write_all(arg.as_bytes()).await?;
                }
            }
            Command::Unknown(cmd) => {
                println!("unexpected command from address {address}: {cmd}");
            }
            Command::Empty => {
                println!("empty command from address {address}")
            }
        }
    }

    Ok(())
}
