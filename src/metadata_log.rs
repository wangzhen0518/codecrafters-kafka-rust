use std::{fs, io::Cursor, path::Path};

use bytes::Buf;

use crate::{
    common_struct::RecordBatch,
    decode::{Decode, DecodeResult},
    describe_topic_partitions::init_topic_partitions,
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
