use std::io;

use crate::encode::Encode;
use crate::request_message::RequestMessage;

const API_VERIONS_API_KEY: i16 = 18;
const API_VERSIONS_MIN_VERSION: i16 = 0;
const API_VERSIONS_MAX_VERSION: i16 = 4;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug, Encode)]
pub struct ResponseMessage {
    message_size: u32,
    header: ResponseHeaderV0,
    body: ResponseBody,
}

#[derive(Debug, Encode)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsV4(ApiVersionsV4),
}

#[derive(Debug, Encode)]
pub struct ApiVersionsV4 {
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
}

#[derive(Debug, Encode)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ResponseMessage {
    pub fn new(correlation_id: i32, body: ResponseBody) -> Self {
        let header = ResponseHeaderV0::new(correlation_id);
        ResponseMessage {
            message_size: 0,
            header,
            body,
        }
    }

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

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeaderV0 { correlation_id }
    }
}

impl Encode for ResponseBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsV4(inner) => inner.encode(),
        }
    }
}

impl ApiVersionsV4 {
    pub fn new(error_code: i16, api_keys: Vec<ApiKey>, throttle_time_ms: i32) -> Self {
        Self {
            error_code,
            api_keys,
            throttle_time_ms,
        }
    }
}

impl ApiKey {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }
}

pub async fn execute_request(request: &RequestMessage) -> io::Result<ResponseMessage> {
    match request.header.request_api_key {
        API_VERIONS_API_KEY => {
            let request_api_version = request.header.request_api_version;
            let (error_code, api_keys) = if request_api_version >= API_VERSIONS_MIN_VERSION
                && request.header.request_api_version <= API_VERSIONS_MAX_VERSION
            {
                (
                    0,
                    vec![ApiKey::new(
                        API_VERIONS_API_KEY,
                        API_VERSIONS_MIN_VERSION,
                        API_VERSIONS_MAX_VERSION,
                    )],
                )
            } else {
                (ERROR_UNSUPPORTED_VERSION, vec![])
            };
            Ok(ResponseMessage::new(
                request.header.correlation_id,
                ResponseBody::ApiVersionsV4(ApiVersionsV4::new(error_code, api_keys, 0)),
            ))
        }
        api_key => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("api_key {} has not been implemented", api_key),
        )),
    }
}
