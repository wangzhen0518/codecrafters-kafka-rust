use std::{collections::HashMap, io::Cursor};

use bitflags::bitflags;
use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    api_versions::ApiKey,
    common_struct::TagBuffer,
    decode::{Decode, DecodeError, DecodeResult},
    encode::Encode,
    request_message::RequestHeaderV2,
    response_message::{ResponseBody, ResponseHeader, ResponseMessage},
};

pub const UNKNOWN_TOPIC_ERROR: i16 = 3; //TODO 考虑怎么把错误码和数据结构结合到一起

lazy_static! {
    pub static ref DESCRIBE_TOPIC_PARTITIONS_API_INFO: ApiKey =
        ApiKey::new(75, 0, 0, TagBuffer::new(None));
    pub static ref TOPIC_PARTITIONS: HashMap<String, TopicInfo> = HashMap::from([]);
}

pub struct TopicInfo {
    name: String,
    id: Uuid,
    is_internal: bool,
    partitions_array: Vec<u8>, //TODO 确认内部是什么
    topic_authorized_operations: TopicAuthorizedOperations,
}

#[derive(Debug, Decode, Encode)]
pub struct DescribeTopicPartitionsV0RequestBody {
    topics: Vec<TopicRequest>,
    response_partition_limit: i32,
    cursor: Option<TopicCursor>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Decode, Encode)]
pub struct TopicRequest {
    name: String,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Decode, Encode)]
pub struct TopicCursor {
    topic_name: String,
    partition_index: i32,
    tag_buffer: TagBuffer,
}

impl Encode for Option<TopicCursor> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Some(cursor) => cursor.encode(),
            None => vec![0xff],
        }
    }
}

impl Decode for Option<TopicCursor> {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let cursor = u8::decode(buffer)?;
        if cursor == 0xff {
            Ok(None)
        } else {
            Ok(Some(TopicCursor::decode(buffer)?))
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct DescribeTopicPartitionsV0ResponseBody {
    throttle_time: i32,
    topic_array: Vec<TopicResponse>,
    next_curor: Option<TopicCursor>,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct TopicResponse {
    error_code: i16,
    name: String,
    id: Uuid,
    is_internal: bool,
    partitions_array: Vec<u8>, //TODO 确认内部是什么
    topic_authorized_operations: TopicAuthorizedOperations,
    tag_buffer: TagBuffer,
}

bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct TopicAuthorizedOperations: u32{
        const UNKNOWN = 1 << 0;
        const ANY = 1 << 1;
        const ALL = 1 << 2;
        const READ = 1 << 3;
        const WRITE = 1 << 4;
        const CREATE = 1 << 5;
        const DELETE = 1 << 6;
        const ALTER = 1 << 7;
        const DESCRIBE = 1 << 8;
        const CLUSTER_ACTION = 1 << 9;
        const DESCRIBE_CONFIGS = 1 << 10;
        const ALTER_CONFIGS = 1 << 11;
        const IDEMPOTENT_WRITE = 1 << 12;
        const CREATE_TOKENS = 1 << 13;
        const DESCRIBE_TOKENS = 1 << 14;
    }
}

impl Default for TopicAuthorizedOperations {
    fn default() -> Self {
        TopicAuthorizedOperations::from_bits_retain(0x0000_0df8)
    }
}

impl Encode for TopicAuthorizedOperations {
    fn encode(&self) -> Vec<u8> {
        self.bits().encode()
    }
}

impl Decode for TopicAuthorizedOperations {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let flags = u32::decode(buffer)?;
        TopicAuthorizedOperations::from_bits(flags).ok_or(DecodeError::Other(
            format!(
                "TopicAuthorizedOperations contains unknown bits: {:#08x}",
                flags
            )
            .into(),
        ))
    }
}

pub fn execute_describe_topic_partitions(
    header: &RequestHeaderV2,
    body: &DescribeTopicPartitionsV0RequestBody,
) -> ResponseMessage {
    let _request_api_version = header.request_api_version; // TODO 需要校验版本吗
    let correlation_id = header.correlation_id;

    let mut topic_array = vec![];
    for topic_request in body.topics.iter() {
        let topic_resp = if let Some(topic_info) = TOPIC_PARTITIONS.get(&topic_request.name) {
            TopicResponse {
                error_code: 0,
                name: topic_info.name.clone(),
                id: topic_info.id,
                is_internal: topic_info.is_internal,
                partitions_array: topic_info.partitions_array.clone(),
                topic_authorized_operations: topic_info.topic_authorized_operations,
                tag_buffer: TagBuffer::new(None),
            }
        } else {
            TopicResponse {
                error_code: UNKNOWN_TOPIC_ERROR,
                name: topic_request.name.clone(),
                id: Uuid::nil(),
                is_internal: false,
                partitions_array: vec![],
                topic_authorized_operations: TopicAuthorizedOperations::default(),
                tag_buffer: TagBuffer::new(None),
            }
        };
        topic_array.push(topic_resp);
    }

    ResponseMessage::new(
        ResponseHeader::new_v1(correlation_id),
        ResponseBody::DescribeTopicPartitionsV0(DescribeTopicPartitionsV0ResponseBody {
            throttle_time: 0,
            topic_array,
            next_curor: None,
            tag_buffer: TagBuffer::new(None),
        }),
    )
}
