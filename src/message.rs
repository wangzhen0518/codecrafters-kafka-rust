use bincode::Options;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HeaderV0 {
    correlation_id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseMessage {
    message_size: u32,
    header: HeaderV0,
    body: Vec<u8>,
}

impl HeaderV0 {
    pub fn new(correlation_id: u32) -> Self {
        HeaderV0 { correlation_id }
    }
}

impl ResponseMessage {
    pub fn new(correlation_id: u32, body: Vec<u8>) -> Self {
        let message_size = 4_u32 + body.len() as u32;
        let header = HeaderV0::new(correlation_id);
        ResponseMessage {
            message_size,
            header,
            body,
        }
    }

    pub fn to_bytes(&self) -> bincode::Result<Vec<u8>> {
        bincode::options()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(self)
    }
}
