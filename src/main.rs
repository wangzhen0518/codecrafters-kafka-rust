use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

mod message;
mod utils;

use message::ResponseMessage;

async fn process(mut socket: TcpStream) {
    let request = message::parse_input(&mut socket)
        .await
        .expect("Failed to parse request");

    let response = ResponseMessage::new(request.header.correlation_id, Vec::new());
    let binary_code = response.to_bytes().expect("Failed to encode");
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
