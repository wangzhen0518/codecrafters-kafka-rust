use std::io::{self, Cursor};

use crate::{
    common_struct::TagBuffer,
    decode::{Decode, DecodeResult},
    encode::Encode,
    response_message::API_VERSIONS_API_INFO,
};

#[derive(Debug, Encode)]
pub struct RequestMessage {
    #[allow(dead_code)]
    pub message_size: u32,
    pub header: RequestHeaderV2,
    pub body: RequestBody,
}

impl RequestMessage {
    pub fn as_bytes(&mut self) -> Vec<u8> {
        if self.message_size == 0 {
            let mut encode_header = self.header.encode();
            let mut encode_body = self.body.encode();

            self.message_size = (encode_header.len() + encode_body.len()) as u32;
            let mut encode_vec = self.message_size.to_be_bytes().to_vec();
            encode_vec.append(&mut encode_header);
            encode_vec.append(&mut encode_body);

            encode_vec
        } else {
            self.encode()
        }
    }
}

impl Decode for RequestMessage {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let message_size = u32::decode(buffer)?;
        let header = RequestHeaderV2::decode(buffer)?;
        let body = if header.request_api_key == API_VERSIONS_API_INFO.api_key {
            RequestBody::ApiVersionsV4(ApiVersionsV4ReqeustBody::decode(buffer)?)
        } else {
            unimplemented!()
        };
        Ok(RequestMessage {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug, Decode, Encode)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: HeaderClientId,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Encode)]
pub struct HeaderClientId {
    pub id: String,
}

impl Decode for HeaderClientId {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let length = u16::decode(buffer)?;
        let mut string_buffer = vec![0; length as usize];
        <Cursor<&[u8]> as io::Read>::read_exact(buffer, &mut string_buffer)?;
        let id = String::from_utf8(string_buffer)?;
        Ok(HeaderClientId { id })
    }
}

#[derive(Debug)]
pub enum RequestBody {
    ApiVersionsV4(ApiVersionsV4ReqeustBody),
}

impl Encode for RequestBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            RequestBody::ApiVersionsV4(body) => body.encode(),
        }
    }
}

#[derive(Debug, Decode, Encode)]
pub struct ApiVersionsV4ReqeustBody {
    pub client_id: String,
    pub client_software_version: String,
    pub tag_buffer: TagBuffer,
}

pub fn request_api_versions(request_api_version: i16) -> RequestMessage {
    RequestMessage {
        message_size: 0,
        header: RequestHeaderV2 {
            request_api_key: API_VERSIONS_API_INFO.api_key,
            request_api_version,
            correlation_id: 0,
            client_id: HeaderClientId {
                id: "myclient".to_string(),
            },
            tag_buffer: TagBuffer::new(None),
        },
        body: RequestBody::ApiVersionsV4(ApiVersionsV4ReqeustBody {
            client_id: "myclient".to_string(),
            client_software_version: "0.1".to_string(),
            tag_buffer: TagBuffer::new(None),
        }),
    }
}
