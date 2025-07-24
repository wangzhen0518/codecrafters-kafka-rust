use crate::encode::Encode;

#[derive(Debug, Encode)]
pub struct TagField {
    fields: Option<Vec<TagSection>>,
}

#[derive(Debug, Encode)]
pub struct TagSection {
    tag: u8,
    data: Vec<u8>,
}

impl TagField {
    pub fn new(fields: Option<Vec<TagSection>>) -> Self {
        Self { fields }
    }
}

impl TagSection {
    pub fn new(tag: u8, data: Vec<u8>) -> Self {
        Self { tag, data }
    }
}
