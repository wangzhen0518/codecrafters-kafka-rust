use std::{error, io};

use bincode::{Decode, Encode};
use tokio::{io::AsyncReadExt, net::TcpStream};

static SERDE_CONFIG: bincode::config::Configuration<
    bincode::config::BigEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_big_endian()
    .with_fixed_int_encoding();

#[derive(Debug, Encode)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

#[derive(Debug, Encode)]
pub struct ResponseMessage {
    message_size: i32,
    header: ResponseHeaderV0,
    body: Vec<u8>,
}

#[derive(Debug)]
pub struct NullableString {
    inner: String,
}

#[derive(Debug)]
pub struct CompactArray<T> {
    inner: Vec<T>,
}

#[derive(Debug, Decode)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    // pub client_id: Option<String>,
    // pub tag_buffer: Option<Vec<u8>>,
}

#[derive(Debug, Decode)]
pub struct RequestMessage {
    pub message_size: i32,
    pub header: RequestHeaderV2,
    // pub body: Vec<u8>,
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeaderV0 { correlation_id }
    }
}

impl ResponseMessage {
    pub fn new(correlation_id: i32, body: Vec<u8>) -> Self {
        let message_size = 4_i32 + body.len() as i32;
        let header = ResponseHeaderV0::new(correlation_id);
        ResponseMessage {
            message_size,
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, io::Error> {
        bincode::encode_to_vec(self, SERDE_CONFIG)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }
}

// impl Decode for NullableString {
//     fn decode<D: bincode::de::Decoder<Context = Context>>(
//         decoder: &mut D,
//     ) -> Result<Self, bincode::error::DecodeError> {
//     }
// }

impl RequestHeaderV2 {
    fn new(
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        // client_id: Option<String>,
        // tag_buffer: Option<Vec<u8>>,
    ) -> Self {
        RequestHeaderV2 {
            request_api_key,
            request_api_version,
            correlation_id,
            // client_id,
            // tag_buffer,
        }
    }
}

impl RequestMessage {
    fn new(
        message_size: i32,
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        // client_id: Option<String>,
        // tag_buffer: Option<Vec<u8>>,
        // body: Vec<u8>,
    ) -> Self {
        RequestMessage {
            message_size,
            header: RequestHeaderV2::new(
                request_api_key,
                request_api_version,
                correlation_id,
                // client_id,
                // tag_buffer,
            ),
            // body,
        }
    }
}

pub async fn parse_input(socket: &mut TcpStream) -> Result<RequestMessage, Box<dyn error::Error>> {
    let message_size = socket.read_i32().await?;
    tracing::debug!("message size: {}", message_size as usize);
    let mut buffer = vec![0; message_size as usize];
    // let mut buffer = Vec::with_capacity(message_size as usize);
    buffer.splice(0..4, message_size.to_be_bytes());
    let _num = socket.read_exact(&mut buffer[4..]).await?;
    let (request_message, _parse_message_size) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)?;

    Ok(request_message)
}
