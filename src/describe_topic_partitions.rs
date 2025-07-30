use std::io::Cursor;

use bitflags::bitflags;
use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    api_versions::{ApiKey, ApiVersionsResponseBodyV4, UNSUPPORTED_VERSION_ERROR},
    common_struct::{CompactArray, CompactString, TagBuffer},
    decode::{Decode, DecodeError, DecodeResult},
    encode::Encode,
    metadata_log::TOPIC_PARTITION_MAP,
    request_message::RequestHeaderV2,
    response_message::{ResponseBody, ResponseHeader, ResponseMessage},
};

pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3; //TODO 考虑怎么把错误码和数据结构结合到一起

lazy_static! {
    pub static ref DESCRIBE_TOPIC_PARTITIONS_API_INFO: ApiKey =
        ApiKey::new(75, 0, 0, TagBuffer::default());
}

pub struct TopicInfo {
    pub name: CompactString,
    pub id: Uuid,
    pub is_internal: bool,
    pub partitions_array: CompactArray<TopicPartition>,
    pub topic_authorized_operations: TopicAuthorizedOperations,
}

#[derive(Debug, Decode, Encode)]
pub struct DescribeTopicPartitionsRequestBodyV0 {
    topics: CompactArray<TopicRequest>,
    response_partition_limit: i32,
    cursor: OptionTopicCursor,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Decode, Encode)]
pub struct TopicRequest {
    //TODO 考虑是否需要修改名称
    name: CompactString,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Decode, Encode)]
pub struct TopicCursor {
    topic_name: CompactString,
    partition_index: i32,
    tag_buffer: TagBuffer,
}

#[derive(Debug)]
pub struct OptionTopicCursor {
    inner: Option<TopicCursor>,
}

impl OptionTopicCursor {
    pub fn new(inner: Option<TopicCursor>) -> Self {
        Self { inner }
    }
}

impl Default for OptionTopicCursor {
    fn default() -> Self {
        OptionTopicCursor::new(None)
    }
}

impl Encode for OptionTopicCursor {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            Some(cursor) => cursor.encode(),
            None => vec![0xff],
        }
    }
}

impl Decode for OptionTopicCursor {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let cursor = u8::decode(buffer)?;
        let inner = if cursor == 0xff {
            None
        } else {
            Some(TopicCursor::decode(buffer)?)
        };
        Ok(OptionTopicCursor::new(inner))
    }
}

#[derive(Debug, Encode, Decode)]
pub struct DescribeTopicPartitionsResponseBodyV0 {
    throttle_time: i32,
    topic_array: CompactArray<TopicResponse>,
    next_curor: OptionTopicCursor,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Encode, Decode)]
pub struct TopicResponse {
    error_code: i16,
    name: CompactString,
    id: Uuid,
    is_internal: bool,
    partitions_array: CompactArray<TopicPartition>,
    topic_authorized_operations: TopicAuthorizedOperations,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TopicPartition {
    pub error_code: i16,
    pub index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub repica_nodes: CompactArray<RepicaNode>,
    pub isr_nodes: CompactArray<RepicaNode>,
    pub eligible_leader_replicas: CompactArray<RepicaNode>,
    pub last_known_elr: CompactArray<RepicaNode>,
    pub offline_replicas: CompactArray<RepicaNode>,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RepicaNode {
    id: i32,
}

impl RepicaNode {
    pub fn new(id: i32) -> Self {
        Self { id }
    }
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
    body: &DescribeTopicPartitionsRequestBodyV0,
) -> ResponseMessage {
    let request_api_version = header.request_api_version;
    let correlation_id = header.correlation_id;

    if request_api_version < DESCRIBE_TOPIC_PARTITIONS_API_INFO.min_version
        || request_api_version > DESCRIBE_TOPIC_PARTITIONS_API_INFO.max_version
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

    let mut describe_topics = vec![];
    if let Some(topics) = body.topics.as_ref() {
        for request_topic in topics.iter() {
            let resp_topic = if let Some(topic_info) = TOPIC_PARTITION_MAP
                .lock()
                .expect("Failed to get TOPIC_PARTITIONS")
                .get(&request_topic.name)
            {
                TopicResponse {
                    error_code: 0,
                    name: topic_info.name.clone(),
                    id: topic_info.id,
                    is_internal: topic_info.is_internal,
                    partitions_array: topic_info.partitions_array.clone(),
                    topic_authorized_operations: topic_info.topic_authorized_operations,
                    tag_buffer: TagBuffer::default(),
                }
            } else {
                TopicResponse {
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    name: request_topic.name.clone(),
                    id: Uuid::nil(),
                    is_internal: false,
                    partitions_array: CompactArray::empty(),
                    topic_authorized_operations: TopicAuthorizedOperations::default(),
                    tag_buffer: TagBuffer::default(),
                }
            };
            describe_topics.push(resp_topic);
        }
    }

    ResponseMessage::new(
        ResponseHeader::new_v1(correlation_id),
        ResponseBody::DescribeTopicPartitionsV0(DescribeTopicPartitionsResponseBodyV0 {
            throttle_time: 0,
            topic_array: CompactArray::new(Some(describe_topics)),
            next_curor: OptionTopicCursor::default(),
            tag_buffer: TagBuffer::default(),
        }),
    )
}
