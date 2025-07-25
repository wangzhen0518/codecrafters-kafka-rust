use std::io;
use std::{collections::HashMap, io::Cursor};

use lazy_static::lazy_static;

use crate::{
    common_struct::TagBuffer,
    decode::{Decode, DecodeResult},
    encode::Encode,
    request_message::RequestMessage,
};

#[derive(Debug, Encode)]
pub struct ResponseMessage {
    message_size: u32,
    header: ResponseHeader,
    body: ResponseBody,
}

impl ResponseMessage {
    pub fn new(header: ResponseHeader, body: ResponseBody) -> Self {
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

    pub fn decode(buffer: &mut Cursor<&[u8]>, request_api_key: i16) -> DecodeResult<Self> {
        let message_size = u32::decode(buffer)?;
        let header = ResponseHeader::ResponseHeaderV1(ResponseHeaderV1::decode(buffer)?);
        let body = if request_api_key == API_VERSIONS_API_INFO.api_key {
            ResponseBody::ApiVersionsV4(ApiVersionsV4ResponseBody::decode(buffer)?)
        } else {
            unimplemented!()
        };
        Ok(ResponseMessage {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug)]
pub enum ResponseHeader {
    ResponseHeaderV0(ResponseHeaderV0),
    ResponseHeaderV1(ResponseHeaderV1),
}

impl ResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        ResponseHeader::ResponseHeaderV0(ResponseHeaderV0 { correlation_id })
    }

    pub fn new_v1(correlation_id: i32) -> Self {
        ResponseHeader::ResponseHeaderV1(ResponseHeaderV1 {
            correlation_id,
            tag_buffer: TagBuffer::new(None),
        })
    }
}

impl Encode for ResponseHeader {
    fn encode(&self) -> Vec<u8> {
        match self {
            ResponseHeader::ResponseHeaderV0(header) => header.encode(),
            ResponseHeader::ResponseHeaderV1(header) => header.encode(),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeaderV0 { correlation_id }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct ResponseHeaderV1 {
    correlation_id: i32,
    tag_buffer: TagBuffer,
}

impl ResponseHeaderV1 {
    pub fn new(correlation_id: i32, tag_buffer: TagBuffer) -> Self {
        ResponseHeaderV1 {
            correlation_id,
            tag_buffer,
        }
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsV4(ApiVersionsV4ResponseBody),
}

impl Encode for ResponseBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsV4(inner) => inner.encode(),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct ApiVersionsV4ResponseBody {
    error_code: i16,
    api_keys: Vec<ApiKey>,
    throttle_time_ms: i32,
    tag_buffer: TagBuffer,
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TagBuffer,
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
    pub static ref API_VERSIONS_API_INFO: ApiKey = ApiKey::new(18, 0, 4, TagBuffer::new(None));
    pub static ref DESCRIBE_TOPIC_PARTITIONS_API_INFO: ApiKey =
        ApiKey::new(75, 0, 0, TagBuffer::new(None));
    pub static ref SUPPORT_APIS: HashMap<i16, ApiKey> = HashMap::from([
        (API_VERSIONS_API_INFO.api_key, API_VERSIONS_API_INFO.clone()),
        (
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key,
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.clone(),
        ),
    ]);
}

pub fn execute_api_verions(request: &RequestMessage) -> ResponseMessage {
    let request_api_version = request.header.request_api_version();
    let correlation_id = request.header.correlation_id();
    let (error_code, mut api_keys) = if request_api_version >= API_VERSIONS_API_INFO.min_version
        && request_api_version <= API_VERSIONS_API_INFO.max_version
    {
        (0, SUPPORT_APIS.values().cloned().collect())
    } else {
        (UNSUPPORTED_VERSION_ERROR, vec![])
    };
    api_keys.sort();

    ResponseMessage::new(
        ResponseHeader::new_v1(correlation_id),
        ResponseBody::ApiVersionsV4(ApiVersionsV4ResponseBody::new(
            error_code,
            api_keys,
            0,
            TagBuffer::new(None),
        )),
    )
}

pub async fn execute_request(request: &RequestMessage) -> io::Result<ResponseMessage> {
    let request_api_key = request.header.request_api_key();
    if request_api_key == API_VERSIONS_API_INFO.api_key {
        Ok(execute_api_verions(request))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "request_api_key {} has not been implemented",
                request_api_key
            ),
        ))
    }
}
