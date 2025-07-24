use crate::{decode::Decode, encode::Encode};

#[derive(Debug, Encode, Decode)]
pub struct TagBuffer {
    fields: Option<Vec<TagSection>>,
}

#[derive(Debug, Encode, Decode)]
pub struct TagSection {
    tag: u8,
    data: Vec<u8>,
}

impl TagBuffer {
    pub fn new(fields: Option<Vec<TagSection>>) -> Self {
        Self { fields }
    }
}

impl TagSection {
    pub fn new(tag: u8, data: Vec<u8>) -> Self {
        Self { tag, data }
    }
}
