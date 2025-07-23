use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod message;
mod utils;

use message::ResponseMessage;

async fn process(mut socket: TcpStream) {
    let mut buffer = [0; 1024];
    let num = socket
        .read(&mut buffer)
        .await
        .expect("Failed to read input");

    tracing::debug!("Receive {} bytes:\n{:?}", num, &buffer[..num]);

    let response = ResponseMessage::new(7, Vec::new());
    let binary_code = response.to_bytes().expect("Failed to encode");

    tracing::debug!("Response {:?}", binary_code);

    socket
        .write_all(&binary_code)
        .await
        .expect("Failed to write response");
}

#[tokio::main]
async fn main() {
    utils::config_logger();

    let listener = TcpListener::bind("127.0.0.1:9092")
        .await
        .expect("Failed to bind to 127.0.0.1:9092");

    loop {
        match listener.accept().await {
            Ok((socket, _addr)) => {
                tracing::info!("Connect with {:?}", socket);
                tokio::spawn(process(socket));
            }
            Err(err) => tracing::error!("Connect error: {:?}", err),
        }
    }
}
