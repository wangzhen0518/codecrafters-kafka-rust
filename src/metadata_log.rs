use std::{
    fs,
    io::{Cursor, Seek},
    path::Path,
};

use bitflags::bitflags;
use bytes::Buf;
use uuid::Uuid;

use crate::{
    common_struct::{Array, CompactArray, CompactString, TagBuffer, Varint},
    decode::{Decode, DecodeError, DecodeResult},
    describe_topic_partitions::{init_topic_partitions, RepicaNode},
    encode::Encode,
};

#[derive(Debug)]
pub struct MetadataLog {
    record_batches: Vec<RecordBatch>,
}

impl MetadataLog {
    pub fn new(record_batches: Vec<RecordBatch>) -> Self {
        Self { record_batches }
    }

    pub fn get_record_batches(&self) -> &Vec<RecordBatch> {
        &self.record_batches
    }
}

#[derive(Debug, Encode, Decode)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic_byte: i8,
    pub crc: i32,
    pub attributes: MetadataAttributes,
    pub last_offset_data: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Array<Record>,
}

impl RecordBatch {
    pub fn get_records(&self) -> &Array<Record> {
        &self.records
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct MetadataAttributes: u16{
        const NO_COMPRESSION = 0b000;
        const GZIP = 0b001;
        const SNAPPY = 0b010;
        const LZ4 = 0b011;
        const ZSTD = 0b100;
        const TIMESTAMP_TYPE = 1 << 3;
        const IS_TRANSACTIONAL = 1 << 4;
        const IS_CONTROL_BATCH = 1 << 5;
        const HAS_DELETE_HORIZON_MS = 1 << 6;
    }
}

impl Encode for MetadataAttributes {
    fn encode(&self) -> Vec<u8> {
        self.bits().encode()
    }
}

impl Decode for MetadataAttributes {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let flags = u16::decode(buffer)?;
        MetadataAttributes::from_bits(flags).ok_or(DecodeError::Other(
            format!("MetadataAttributes contains unknown bits: {:#08x}", flags).into(),
        ))
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Record {
    pub length: Varint, // signed
    pub attributes: i8,
    pub timestamp_delta: i8,
    pub offset_delta: i8,
    pub key: CompactArray<u8>,
    pub value: RecordValue,
    pub headers_array_count: CompactArray<u8>, //TODO 确定内部类型
}

impl Record {
    pub fn get_value(&self) -> &RecordValue {
        &self.value
    }
}

pub struct RecordType;

impl RecordType {
    pub const TOPIC_RECORD: i8 = 0x02;
    pub const PARITION_RECORD: i8 = 0x03;
    pub const FEATURE_LEVEL_RECORD: i8 = 0x0c;
}

#[derive(Debug, Clone)]
pub enum RecordValue {
    Topic(TopicRecord),
    Partition(ParitionRecord),
    FeatureLevel(FeatureLevelRecord),
    Unknown,
}

impl RecordValue {
    pub fn is_unknown(&self) -> bool {
        matches!(self, RecordValue::Unknown)
    }
}

impl Encode for RecordValue {
    fn encode(&self) -> Vec<u8> {
        match &self {
            RecordValue::Topic(record) => record.encode(),
            RecordValue::Partition(record) => record.encode(),
            RecordValue::FeatureLevel(record) => record.encode(),
            RecordValue::Unknown => vec![0x00],
        }
    }
}

impl Decode for RecordValue {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let value_length = Varint::decode(buffer)?;
        let _frame_version = i8::decode(buffer)?;
        let record_type = i8::decode(buffer)?;
        buffer.seek_relative(-2).expect("Failed to seek");
        let record_value = match record_type {
            RecordType::TOPIC_RECORD => RecordValue::Topic(TopicRecord::decode(buffer)?),
            RecordType::PARITION_RECORD => RecordValue::Partition(ParitionRecord::decode(buffer)?),
            RecordType::FEATURE_LEVEL_RECORD => {
                RecordValue::FeatureLevel(FeatureLevelRecord::decode(buffer)?)
            }
            record_type => {
                tracing::error!("Unknown record type: {}", record_type);
                buffer
                    .seek_relative(value_length.as_i64())
                    .expect("Failed to seek");
                RecordValue::Unknown
            }
        };
        Ok(record_value)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TopicRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    pub name: CompactString,
    pub id: Uuid,
    pub tag_buffers: TagBuffer,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ParitionRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    pub parition_id: i32,
    pub topic_uuid: Uuid,
    pub replica_nodes: CompactArray<RepicaNode>,
    pub isr_nodes: CompactArray<RepicaNode>,
    pub removing_replicas_nodes: CompactArray<RepicaNode>,
    pub adding_replicas_nodes: CompactArray<RepicaNode>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub directories: CompactArray<Directory>,
    pub tag_buffers: TagBuffer,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Directory {
    id: Uuid,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct FeatureLevelRecord {
    frame_version: i8,
    record_type: i8,
    version: i8,
    name: CompactString,
    feature_level: i16,
    tag_buffers: TagBuffer,
}

pub fn init_read_metadata_log() -> DecodeResult<()> {
    let metadata_log_file =
        Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
    // let metadata_log_file = Path::new("tmp/demo.log");
    if metadata_log_file.exists() {
        let log_content = fs::read(metadata_log_file)?; //TODO 支持异步

        let mut buffer = Cursor::new(log_content.as_ref());
        let mut record_batches = vec![];
        while buffer.has_remaining() {
            let record_batch = RecordBatch::decode(&mut buffer)?; // loop 循环 decode
            record_batches.push(record_batch);
        }

        let metadata_log = MetadataLog::new(record_batches);
        // tracing::debug!("Metadata Log:\n{:#?}", &metadata_log);

        init_topic_partitions(&metadata_log);
    } else {
        panic!(
            "Cannot find metadata log file: {}",
            metadata_log_file.to_string_lossy()
        );
    }

    Ok(())
}
