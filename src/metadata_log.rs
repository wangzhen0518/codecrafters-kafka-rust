use std::{
    collections::HashMap,
    fs,
    io::Cursor,
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Buf;
use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    common_struct::{CompactArray, CompactString, RecordBatch, RecordValue},
    decode::{Decode, DecodeError, DecodeResult},
    describe_topic_partitions::{TopicAuthorizedOperations, TopicInfo, TopicPartition},
};

lazy_static! {
    pub static ref TOPIC_ID_NAME_MAP: Arc<Mutex<HashMap<Uuid, CompactString>>> =
        Arc::new(Mutex::new(HashMap::new()));
    pub static ref TOPIC_INFO_MAP: Arc<Mutex<HashMap<CompactString, TopicInfo>>> =
        Arc::new(Mutex::new(HashMap::new()));
    pub static ref TOPIC_RECORD_BATCH_MAP: Arc<Mutex<HashMap<CompactString, Vec<RecordBatch>>>> =
        Arc::new(Mutex::new(HashMap::new()));
}

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

fn init_internel_states(metadata_log: &MetadataLog) {
    let mut topic_info_array = vec![];
    for record_batch in metadata_log.get_record_batches() {
        let mut found = false;
        let mut topic_info = TopicInfo {
            name: CompactString::default(),
            id: Uuid::nil(),
            is_internal: false,
            partitions_array: CompactArray::empty(),
            topic_authorized_operations: TopicAuthorizedOperations::default(),
        };
        if let Some(records) = record_batch.get_records().get_inner() {
            for record in records {
                match record.get_value() {
                    RecordValue::Topic(topic) => {
                        found = true;
                        topic_info.name = topic.name.clone();
                        topic_info.id = topic.id;
                    }
                    RecordValue::Partition(partition) => {
                        found = true;
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
        if found {
            let mut topic_record_batch_map = TOPIC_RECORD_BATCH_MAP
                .lock()
                .expect("Failed to get TOPIC_RECORD_BATCH_MAP lock");
            let mut record_batch = record_batch.clone();
            match topic_record_batch_map.get_mut(&topic_info.name) {
                None => {
                    record_batch.base_offset = 0;
                    topic_record_batch_map.insert(topic_info.name.clone(), vec![record_batch]);
                }
                Some(array) => {
                    record_batch.base_offset = array.len() as i64;
                    array.push(record_batch);
                }
            }

            topic_info_array.push(topic_info);
        }
    }

    let mut topic_id_name_map = TOPIC_ID_NAME_MAP
        .lock()
        .expect("Failed to get TOPIC_ID_NAME_MAP lock");
    let mut topic_partition_map = TOPIC_INFO_MAP
        .lock()
        .expect("Failed to get TOPIC_PARTITION_MAP's lock");
    for topic_info in topic_info_array {
        topic_id_name_map.insert(topic_info.id, topic_info.name.clone());
        topic_partition_map.insert(topic_info.name.clone(), topic_info);
    }
}

pub fn read_record_batches(path: &Path) -> DecodeResult<Vec<RecordBatch>> {
    if path.exists() {
        let log_content = fs::read(path)?; //TODO 支持异步
                                           // tracing::debug!(
                                           //     "Read: {:?}\nContent:\n{}",
                                           //     path,
                                           //     display_bytes(&log_content)
                                           // );

        let mut buffer = Cursor::new(log_content.as_ref());
        let mut record_batches = vec![];
        while buffer.has_remaining() {
            let record_batch = RecordBatch::decode(&mut buffer)?; // loop 循环 decode
            record_batches.push(record_batch);
        }
        Ok(record_batches)
    } else {
        Err(DecodeError::Other(
            format!("Cannot find metadata log file: {}", path.to_string_lossy()).into(),
        ))
    }
}

pub fn init_read_metadata_log() -> DecodeResult<()> {
    let metadata_log_file =
        Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
    // let metadata_log_file = Path::new("tmp/demo.bin");
    let record_batches = read_record_batches(metadata_log_file)?;
    let metadata_log = MetadataLog::new(record_batches);
    init_internel_states(&metadata_log);

    Ok(())
}
