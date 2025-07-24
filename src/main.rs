#![allow(dead_code)]

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

mod common_struct;
mod decode;
mod encode;
mod request_message;
mod response_message;
mod utils;

async fn process(mut socket: TcpStream) {
    let request = request_message::parse_input(&mut socket)
        .await
        .expect("Failed to parse request");

    let mut response = response_message::execute_request(&request)
        .await
        .expect("Failed to execute request");
    let binary_code = response.as_bytes();
    socket
        .write_all(&binary_code)
        .await
        .expect("Failed to write response");

    tracing::debug!("Receive Request:\n{:?}", request);
    tracing::debug!("Response:\n{:?}\n{:02x?}", response, binary_code);
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
