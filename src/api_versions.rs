use std::collections::HashMap;

use lazy_static::lazy_static;

use crate::{
    common_struct::{CompactArray, CompactString, TagBuffer},
    decode::Decode,
    describe_topic_partitions::DESCRIBE_TOPIC_PARTITIONS_API_INFO,
    encode::Encode,
    fetch::FETCH_API_INFO,
    request_message::RequestHeaderV2,
    response_message::{ResponseBody, ResponseHeader, ResponseMessage},
};

pub const UNSUPPORTED_VERSION_ERROR: i16 = 35;

lazy_static! {
    pub static ref API_VERSIONS_API_INFO: ApiKey = ApiKey::new(18, 0, 4, TagBuffer::default());
    pub static ref SUPPORT_APIS: HashMap<i16, ApiKey> = HashMap::from([
        (FETCH_API_INFO.api_key, FETCH_API_INFO.clone()),
        (API_VERSIONS_API_INFO.api_key, API_VERSIONS_API_INFO.clone()),
        (
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key,
            DESCRIBE_TOPIC_PARTITIONS_API_INFO.clone(),
        ),
    ]);
}

#[derive(Debug, Decode, Encode)]
pub struct ApiVersionsReqeustBodyV4 {
    pub client_id: CompactString,
    pub client_software_version: CompactString,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct ApiVersionsResponseBodyV4 {
    error_code: i16,
    api_keys: CompactArray<ApiKey>,
    throttle_time_ms: i32,
    tag_buffer: TagBuffer,
}

impl ApiVersionsResponseBodyV4 {
    pub fn new(
        error_code: i16,
        api_keys: CompactArray<ApiKey>,
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

pub fn execute_api_verions(
    header: &RequestHeaderV2,
    _body: &ApiVersionsReqeustBodyV4,
) -> ResponseMessage {
    let request_api_version = header.request_api_version;
    let correlation_id = header.correlation_id;
    let (error_code, mut api_keys) = if request_api_version >= API_VERSIONS_API_INFO.min_version
        && request_api_version <= API_VERSIONS_API_INFO.max_version
    {
        (0, SUPPORT_APIS.values().cloned().collect())
    } else {
        (UNSUPPORTED_VERSION_ERROR, vec![])
    };
    api_keys.sort();

    ResponseMessage::new(
        ResponseHeader::new_v0(correlation_id),
        ResponseBody::ApiVersionsV4(ApiVersionsResponseBodyV4::new(
            error_code,
            CompactArray::new(Some(api_keys)),
            0,
            TagBuffer::default(),
        )),
    )
}
