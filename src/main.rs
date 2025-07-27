#![allow(dead_code)]

use tokio::net::{TcpListener, TcpStream};

use crate::connection::Connection;

mod api_versions;
mod common_struct;
mod connection;
mod decode;
mod encode;
mod request_message;
mod response_message;
mod utils;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

async fn process(socket: TcpStream) {
    let mut connection = Connection::new(socket);
    while let Some(request) = connection
        .read_request()
        .await
        .expect("Failed to read content from socket")
    {
        let mut response = response_message::execute_request(&request)
            .await
            .expect("Failed to execute request");

        connection
            .write_response(&mut response)
            .await
            .expect("Failed to write response");

        tracing::debug!("Receive Request:\n{:#?}", request);
        tracing::debug!("Response:\n{:#?}", response);
    }
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
