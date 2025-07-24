use std::io;

use bincode::Encode;
use serde::Serialize;

use crate::{request_message::RequestMessage, utils::U32_SIZE};

static SERDE_CONFIG: bincode::config::Configuration<
    bincode::config::BigEndian,
    bincode::config::Fixint,
> = bincode::config::standard()
    .with_big_endian()
    .with_fixed_int_encoding();

const API_VERIONS_API_KEY: i16 = 18;
const API_VERSIONS_MIN_VERSION: i16 = 0;
const API_VERSIONS_MAX_VERSION: i16 = 4;
const ERROR_UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug, Encode, Serialize)]
pub struct ResponseMessage {
    message_size: u32,
    header: ResponseHeaderV0,
    body: ResponseBody,
}

#[derive(Debug, Encode, Serialize)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResponseBody {
    ApiVersionsV4(ApiVersionsV4),
}

#[derive(Debug, Encode, Serialize)]
pub struct ApiVersionsV4 {
    error_code: i16,
    api_keys: ApiKey,
    throttle_time_ms: i32,
}

#[derive(Debug, Encode, Serialize)]
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

    #[allow(dead_code)]
    pub fn as_bytes(&mut self) -> io::Result<Vec<u8>> {
        if self.message_size == 0 {
            let header_body_buffer =
                bincode::encode_to_vec((&self.header, &self.body), SERDE_CONFIG)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

            let message_size = header_body_buffer.len() as u32;
            let mut full_buffer = Vec::with_capacity(U32_SIZE + header_body_buffer.len());

            // 3. 编码 message_size (大端序)
            full_buffer.extend_from_slice(&message_size.to_be_bytes());

            // 4. 添加 header 和 body
            full_buffer.extend(header_body_buffer); //TODO 是否有不拷贝的方式

            Ok(full_buffer)
        } else {
            bincode::encode_to_vec(&(*self), SERDE_CONFIG)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
        }
    }

    #[allow(dead_code)]
    pub fn as_serde_bytes(&mut self) -> io::Result<Vec<u8>> {
        if self.message_size == 0 {
            // 先序列化 header 和 body 以计算长度
            let header_body_buffer =
                bincode::serde::encode_to_vec((&self.header, &self.body), SERDE_CONFIG)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            let message_size = header_body_buffer.len() as u32;
            // 然后创建完整消息
            let mut full_buffer = Vec::new();
            full_buffer.extend_from_slice(&message_size.to_be_bytes()); // 写入 message_size (大端序)
            full_buffer.extend_from_slice(&header_body_buffer); // 写入 header 和 body
            Ok(full_buffer)
        } else {
            bincode::serde::encode_to_vec(self, SERDE_CONFIG)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
        }
    }
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeaderV0 { correlation_id }
    }
}

impl Encode for ResponseBody {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        match self {
            ResponseBody::ApiVersionsV4(inner) => inner.encode(encoder),
        }
    }
}

impl ApiVersionsV4 {
    pub fn new(
        error_code: i16,
        api_key: i16,
        min_version: i16,
        max_version: i16,
        throttle_time_ms: i32,
    ) -> Self {
        Self {
            error_code,
            api_keys: ApiKey::new(api_key, min_version, max_version),
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
            let error_code = if request_api_version >= API_VERSIONS_MIN_VERSION
                && request.header.request_api_version <= API_VERSIONS_MAX_VERSION
            {
                0
            } else {
                ERROR_UNSUPPORTED_VERSION
            };
            Ok(ResponseMessage::new(
                request.header.correlation_id,
                ResponseBody::ApiVersionsV4(ApiVersionsV4::new(
                    error_code,
                    API_VERIONS_API_KEY,
                    API_VERSIONS_MIN_VERSION,
                    API_VERSIONS_MAX_VERSION,
                    0,
                )),
            ))
        }
        api_key => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("api_key {} has not been implemented", api_key),
        )),
    }
}
