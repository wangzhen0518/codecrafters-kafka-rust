use std::io;

use bincode::{Decode, Encode};

#[derive(Debug, Encode, Decode)]
pub struct HeaderV0 {
    correlation_id: u32,
}

#[derive(Debug, Encode, Decode)]
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

    pub fn to_bytes(&self) -> Result<Vec<u8>, io::Error> {
        let config = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        bincode::encode_to_vec(self, config)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }
}
