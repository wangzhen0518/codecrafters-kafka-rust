use std::{
    error,
    io::{self, Cursor},
};

use bytes::Buf;
// use bincode::Decode;
use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::{common_struct::TagBuffer, decode::Decode, utils::U32_SIZE};

#[derive(Debug)]
pub struct RequestMessage {
    #[allow(dead_code)]
    pub message_size: u32,
    pub header: RequestHeaderV2,
    pub body: RequestBody,
}

#[derive(Debug, Decode)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: ClientId,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug)]
pub struct ClientId {
    pub id: String,
}

#[derive(Debug)]
pub enum RequestBody {
    ApiVersionsV4(ApiVersionsV4ReqeustBody),
}

#[derive(Debug, Decode)]
pub struct ApiVersionsV4ReqeustBody {
    pub client_id: String,
    pub client_software_version: String,
    pub tag_buffer: TagBuffer,
}

impl Decode for ClientId {
    fn decode(buffer: &mut Cursor<&[u8]>) -> Self {
        let length = buffer.get_u16();
        let mut string_buffer = vec![0; length as usize];
        let _ = <Cursor<&[u8]> as io::Read>::read_exact(buffer, &mut string_buffer); //TODO 异常处理
        let id = String::from_utf8(string_buffer).expect("Invalid UTF-8");
        ClientId { id }
    }
}

impl Decode for RequestMessage {
    fn decode(buffer: &mut Cursor<&[u8]>) -> Self {
        let message_size = buffer.get_u32();
        let header = RequestHeaderV2::decode(buffer);
        let body = match &header.request_api_key {
            &18 => RequestBody::ApiVersionsV4(ApiVersionsV4ReqeustBody::decode(buffer)),
            _ => unimplemented!(),
        };
        RequestMessage {
            message_size,
            header,
            body,
        }
    }
}

pub async fn parse_input(socket: &mut TcpStream) -> Result<RequestMessage, Box<dyn error::Error>> {
    let message_size = socket.read_u32().await?;
    tracing::debug!("message size: {}", message_size as usize);
    let mut buffer = vec![0; message_size as usize + U32_SIZE];
    buffer.splice(..U32_SIZE, message_size.to_be_bytes());
    let _num = socket.read_exact(&mut buffer[U32_SIZE..]).await?;
    let mut cursor_buffer = Cursor::new(buffer.as_slice());
    let request_message = RequestMessage::decode(&mut cursor_buffer);

    Ok(request_message)
}
