use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

use bitflags::bitflags;
use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    api_versions::ApiKey,
    common_struct::{CompactArray, CompactString, TagBuffer},
    decode::{Decode, DecodeError, DecodeResult},
    encode::Encode,
    metadata_log::{MetadataLog, RecordValue},
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
    pub name: CompactString,
    pub id: Uuid,
    pub is_internal: bool,
    pub partitions_array: CompactArray<TopicPartition>,
    pub topic_authorized_operations: TopicAuthorizedOperations,
}

#[derive(Debug, Decode, Encode)]
pub struct DescribeTopicPartitionsV0RequestBody {
    topics: CompactArray<TopicRequest>,
    response_partition_limit: i32,
    cursor: OptionTopicCursor,
    tag_buffer: TagBuffer,
}

#[derive(Debug, Decode, Encode)]
pub struct TopicRequest {
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
pub struct DescribeTopicPartitionsV0ResponseBody {
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
    error_code: i16,
    index: i32,
    leader_id: i32,
    leader_epoch: i32,
    repica_nodes: CompactArray<RepicaNode>,
    isr_nodes: CompactArray<RepicaNode>,
    eligible_leader_replicas: CompactArray<RepicaNode>,
    last_known_elr: CompactArray<RepicaNode>,
    offline_replicas: CompactArray<RepicaNode>,
    tag_buffer: TagBuffer,
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

pub fn init_topic_partitions(metadata_log: &MetadataLog) {
    for record_batch in metadata_log.record_batches() {
        let mut topic_info = TopicInfo {
            name: CompactString::default(),
            id: Uuid::nil(),
            is_internal: false,
            partitions_array: CompactArray::empty(),
            topic_authorized_operations: TopicAuthorizedOperations::default(),
        };
        if let Some(records) = record_batch.records().inner() {
            for record in records {
                match record.value() {
                    RecordValue::Topic(topic) => {
                        topic_info.name = topic.name.clone();
                        topic_info.id = topic.id;
                    }
                    RecordValue::Partition(partition) => {
                        let topic_partition = TopicPartition {
                            error_code: 0,                //TODO 包含在哪里
                            index: partition.parition_id, //TODO 是否是同一个属性
                            leader_id: partition.leader_id,
                            leader_epoch: partition.leader_epoch,
                            repica_nodes: partition.replica_nodes.clone(),
                            isr_nodes: partition.isr_nodes.clone(),
                            eligible_leader_replicas: CompactArray::empty(), //TODO 包含在哪里
                            last_known_elr: CompactArray::empty(),           //TODO 包含在哪里
                            offline_replicas: CompactArray::empty(),         //TODO 包含在哪里
                            tag_buffer: partition.tag_buffers.clone(),
                        };
                        topic_info
                            .partitions_array
                            .as_mut()
                            .unwrap()
                            .push(topic_partition);
                    }
                    _ => {}
                }
            }
        }
        TOPIC_PARTITIONS
            .lock()
            .expect("Failed to get TOPIC_PARITIONS's lock")
            .insert(topic_info.name.clone(), topic_info);
    }
}

pub fn execute_describe_topic_partitions(
    header: &RequestHeaderV2,
    body: &DescribeTopicPartitionsV0RequestBody,
) -> ResponseMessage {
    let _request_api_version = header.request_api_version; // TODO 需要校验版本吗
    let correlation_id = header.correlation_id;

    let mut topic_array = vec![];
    for topic_request in body.topics.as_ref().unwrap().iter() {
        let topic_resp = if let Some(topic_info) = TOPIC_PARTITIONS
            .lock()
            .expect("Failed to get TOPIC_PARTITIONS")
            .get(&topic_request.name)
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
                error_code: UNKNOWN_TOPIC_ERROR,
                name: topic_request.name.clone(),
                id: Uuid::nil(),
                is_internal: false,
                partitions_array: CompactArray::empty(),
                topic_authorized_operations: TopicAuthorizedOperations::default(),
                tag_buffer: TagBuffer::default(),
            }
        };
        topic_array.push(topic_resp);
    }

    ResponseMessage::new(
        ResponseHeader::new_v1(correlation_id),
        ResponseBody::DescribeTopicPartitionsV0(DescribeTopicPartitionsV0ResponseBody {
            throttle_time: 0,
            topic_array: CompactArray::new(Some(topic_array)),
            next_curor: OptionTopicCursor::default(),
            tag_buffer: TagBuffer::default(),
        }),
    )
}
