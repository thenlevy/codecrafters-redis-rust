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
        match cmd.as_str() {
            "PING" => {
                output.write_all(b"+PONG\r\n").await?;
            }
            cmd => {
                println!("unexpected command from address {address}: {cmd}");
            }
        }
    }

    Ok(())
}
