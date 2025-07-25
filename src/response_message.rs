use std::collections::HashMap;
use std::io;

use lazy_static::lazy_static;

use crate::common_struct::TagBuffer;
use crate::encode::Encode;
use crate::request_message::RequestMessage;

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
    ApiVersionsV4(ApiVersionsV4ResponseBody),
}

#[derive(Debug, Encode)]
pub struct ApiVersionsV4ResponseBody {
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Clone)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag_buffer: TagBuffer,
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

impl ApiVersionsV4ResponseBody {
    pub fn new(
        error_code: i16,
        api_keys: Vec<ApiKey>,
        throttle_time_ms: i32,
        tag_buffer: TagBuffer,
    ) -> Self {
        Self {
            error_code,
            api_keys,
            throttle_time_ms,
            tag_buffer,
        }
    }
}

impl ApiKey {
    pub fn new(api_key: i16, min_version: i16, max_version: i16, tag_buffer: TagBuffer) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
            tag_buffer,
        }
    }
}

impl PartialEq for ApiKey {
    fn eq(&self, other: &Self) -> bool {
        self.api_key == other.api_key
    }
}

impl PartialOrd for ApiKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.api_key.cmp(&other.api_key))
    }
}

impl Eq for ApiKey {}

impl Ord for ApiKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.api_key.cmp(&other.api_key)
    }
}

const UNSUPPORTED_VERSION_ERROR: i16 = 35;

lazy_static! {
    static ref API_VERSIONS_API_INFO: ApiKey = ApiKey::new(18, 0, 4, TagBuffer::new(None));
    static ref DESCRIBE_TOPIC_PARTITIONS_API_INFO: ApiKey =
        ApiKey::new(75, 0, 0, TagBuffer::new(None));
    static ref SUPPORT_APIS: HashMap<i16, ApiKey> = HashMap::from([
        (API_VERSIONS_API_INFO.api_key, API_VERSIONS_API_INFO.clone()),
        (
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key,
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.clone(),
        ),
    ]);
}

pub async fn execute_request(request: &RequestMessage) -> io::Result<ResponseMessage> {
    match request.header.request_api_key {
        api_key if api_key == API_VERSIONS_API_INFO.api_key => {
            let request_api_version = request.header.request_api_version;
            let (error_code, mut api_keys) = if request_api_version
                >= API_VERSIONS_API_INFO.min_version
                && request.header.request_api_version <= API_VERSIONS_API_INFO.max_version
            {
                (0, SUPPORT_APIS.values().cloned().collect())
            } else {
                (UNSUPPORTED_VERSION_ERROR, vec![])
            };
            api_keys.sort();

            Ok(ResponseMessage::new(
                request.header.correlation_id,
                ResponseBody::ApiVersionsV4(ApiVersionsV4ResponseBody::new(
                    error_code,
                    api_keys,
                    0,
                    TagBuffer::new(None),
                )),
            ))
        }
        api_key => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("api_key {} has not been implemented", api_key),
        )),
    }
}
