use std::error;

use bincode::Decode;
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::utils::U32_SIZE;

static SERDE_CONFIG: bincode::config::Configuration<
    bincode::config::BigEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_big_endian()
    .with_fixed_int_encoding();

#[derive(Debug, Decode)]
pub struct RequestMessage {
    #[allow(dead_code)]
    pub message_size: u32,
    pub header: RequestHeaderV2,
    // pub body: Vec<u8>,
}

#[derive(Debug, Decode)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    // pub client_id: Option<String>,
    // pub tag_buffer: Option<Vec<u8>>,
}

pub async fn parse_input(socket: &mut TcpStream) -> Result<RequestMessage, Box<dyn error::Error>> {
    let message_size = socket.read_u32().await?;
    tracing::debug!("message size: {}", message_size as usize);
    let mut buffer = vec![0; message_size as usize + U32_SIZE];
    // let mut buffer = Vec::with_capacity(message_size as usize);
    buffer.splice(..U32_SIZE, message_size.to_be_bytes());
    let _num = socket.read_exact(&mut buffer[U32_SIZE..]).await?;
    let (request_message, _parse_message_size) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)?;

    Ok(request_message)
}
