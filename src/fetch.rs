use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    api_versions::{ApiKey, ApiVersionsResponseBodyV4, UNSUPPORTED_VERSION_ERROR},
    common_struct::{CompactArray, CompactString, Record, TagBuffer},
    decode::Decode,
    encode::Encode,
    request_message::RequestHeaderV2,
    response_message::{ResponseBody, ResponseHeader, ResponseMessage},
};

pub const INVALID_FETCH_SIZE_ERROR: i16 = 4;
pub const UNKNOWN_TOPIC_ID_ERROR: i16 = 100;

lazy_static! {
    pub static ref FETCH_API_INFO: ApiKey = ApiKey::new(1, 0, 16, TagBuffer::default());
    pub static ref FETCH_TOPIC_PARTITIONS: Arc<Mutex<HashMap<Uuid, CompactArray<FetchPartitionResponse>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

#[derive(Debug, Encode, Decode)]
pub struct FetchRequestBodyV16 {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: CompactArray<FetchTopicRequest>,
    forgotten_topics_data: CompactArray<ForgottenTopicRequest>,
    rack_id: CompactString,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct FetchTopicRequest {
    topic_id: Uuid,
    partitions: CompactArray<FetchPartitionRequest>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct FetchPartitionRequest {
    partition_index: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct ForgottenTopicRequest {
    topic_id: Uuid,
    partitions: i32,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct FetchResponseBodyV16 {
    throttle_time_ms: i32,
    error_code: i16,
    session_id: i32,
    responses: CompactArray<FetchTopicResponse>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct FetchTopicResponse {
    topic_id: Uuid,
    partitions: CompactArray<FetchPartitionResponse>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct FetchPartitionResponse {
    partition_index: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: CompactArray<Transaction>,
    preferred_read_replica: i32,
    records: CompactArray<Record>,
    tag_buffer: TagBuffer,
}

impl FetchPartitionResponse {
    pub fn new_unknown_topic_partition() -> Self {
        FetchPartitionResponse {
            partition_index: 0,
            error_code: UNKNOWN_TOPIC_ID_ERROR,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: CompactArray::empty(),
            preferred_read_replica: 0,
            records: CompactArray::empty(),
            tag_buffer: TagBuffer::default(),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct Transaction {
    producer_id: i64,
    first_offset: i64,
    tag_buffer: TagBuffer,
}

pub fn execute_fetch(header: &RequestHeaderV2, body: &FetchRequestBodyV16) -> ResponseMessage {
    let request_api_version = header.request_api_version;
    let correlation_id = header.correlation_id;

    if request_api_version < FETCH_API_INFO.min_version
        || request_api_version > FETCH_API_INFO.max_version
    {
        return ResponseMessage::new(
            ResponseHeader::new_v0(correlation_id),
            ResponseBody::ApiVersionsV4(ApiVersionsResponseBodyV4::new(
                UNSUPPORTED_VERSION_ERROR,
                CompactArray::new(Some(vec![])),
                0,
                TagBuffer::default(),
            )),
        );
    }

    let mut fetch_topics = vec![];
    if let Some(topics) = body.topics.as_ref() {
        for request_topic in topics.iter() {
            let resp_topic = if let Some(topic_info) = FETCH_TOPIC_PARTITIONS
                .lock()
                .expect("Failed to get TOPIC_PARTITIONS")
                .get(&request_topic.topic_id)
            {
                FetchTopicResponse {
                    topic_id: request_topic.topic_id,
                    partitions: CompactArray::new(Some(vec![
                        FetchPartitionResponse::new_unknown_topic_partition(),
                    ])),
                    tag_buffer: TagBuffer::default(),
                }
            } else {
                FetchTopicResponse {
                    topic_id: request_topic.topic_id,
                    partitions: CompactArray::new(Some(vec![
                        FetchPartitionResponse::new_unknown_topic_partition(),
                    ])),
                    tag_buffer: TagBuffer::default(),
                }
            };
            fetch_topics.push(resp_topic);
        }
    }

    ResponseMessage::new(
        ResponseHeader::new_v1(correlation_id),
        ResponseBody::FetchV16(FetchResponseBodyV16 {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: CompactArray::new(Some(fetch_topics)),
            tag_buffer: TagBuffer::default(),
        }),
    )
}
