use std::io::Cursor;

use crate::{
    api_versions::{ApiVersionsReqeustBodyV4, API_VERSIONS_API_INFO},
    common_struct::{CompactString, KafkaString, TagBuffer},
    decode::{Decode, DecodeResult},
    describe_topic_partitions::{
        DescribeTopicPartitionsRequestBodyV0, DESCRIBE_TOPIC_PARTITIONS_API_INFO,
    },
    encode::Encode,
};

#[derive(Debug, Encode)]
pub struct RequestMessage {
    #[allow(dead_code)]
    pub message_size: u32,
    pub header: RequestHeader,
    pub body: RequestBody,
}

impl RequestMessage {
    pub fn as_bytes(&mut self) -> Vec<u8> {
        if self.message_size == 0 {
            let mut encode_header = self.header.encode();
            let mut encode_body = self.body.encode();

            self.message_size = (encode_header.len() + encode_body.len()) as u32;
            let mut encode_vec = self.message_size.to_be_bytes().to_vec();
            encode_vec.append(&mut encode_header);
            encode_vec.append(&mut encode_body);

            encode_vec
        } else {
            self.encode()
        }
    }
}

impl Decode for RequestMessage {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let message_size = u32::decode(buffer)?;
        let header = RequestHeader::RequestHeaderV2(RequestHeaderV2::decode(buffer)?);
        let body = if header.request_api_key() == API_VERSIONS_API_INFO.api_key {
            RequestBody::ApiVersionsV4(ApiVersionsReqeustBodyV4::decode(buffer)?)
        } else if header.request_api_key() == DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key {
            RequestBody::DescribeTopicPartitionsV0(DescribeTopicPartitionsRequestBodyV0::decode(
                buffer,
            )?)
        } else {
            unimplemented!("Unknown request api key: {}", header.request_api_key());
        };
        Ok(RequestMessage {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug)]
pub enum RequestHeader {
    RequestHeaderV2(RequestHeaderV2),
}

impl RequestHeader {
    pub fn new_v2(
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
        client_id: KafkaString,
        tag_buffer: TagBuffer,
    ) -> Self {
        RequestHeader::RequestHeaderV2(RequestHeaderV2 {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }

    pub fn request_api_key(&self) -> i16 {
        match self {
            RequestHeader::RequestHeaderV2(header) => header.request_api_key,
        }
    }

    pub fn request_api_version(&self) -> i16 {
        match self {
            RequestHeader::RequestHeaderV2(header) => header.request_api_version,
        }
    }

    pub fn correlation_id(&self) -> i32 {
        match self {
            RequestHeader::RequestHeaderV2(header) => header.correlation_id,
        }
    }

    pub fn client_id(&self) -> &str {
        match self {
            RequestHeader::RequestHeaderV2(header) => &header.client_id,
        }
    }
}

impl Encode for RequestHeader {
    fn encode(&self) -> Vec<u8> {
        match self {
            RequestHeader::RequestHeaderV2(header) => header.encode(),
        }
    }
}

#[derive(Debug, Decode, Encode)]
pub struct RequestHeaderV2 {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: KafkaString,
    pub tag_buffer: TagBuffer,
}

#[derive(Debug)]
pub enum RequestBody {
    ApiVersionsV4(ApiVersionsReqeustBodyV4),
    DescribeTopicPartitionsV0(DescribeTopicPartitionsRequestBodyV0),
}

impl Encode for RequestBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            RequestBody::ApiVersionsV4(body) => body.encode(),
            RequestBody::DescribeTopicPartitionsV0(body) => body.encode(),
        }
    }
}

pub fn request_api_versions(request_api_version: i16) -> RequestMessage {
    RequestMessage {
        message_size: 0,
        header: RequestHeader::new_v2(
            API_VERSIONS_API_INFO.api_key,
            request_api_version,
            0,
            KafkaString::new("myclient".to_string()),
            TagBuffer::default(),
        ),
        body: RequestBody::ApiVersionsV4(ApiVersionsReqeustBodyV4 {
            client_id: CompactString::new("myclient".to_string()),
            client_software_version: CompactString::new("0.1".to_string()),
            tag_buffer: TagBuffer::default(),
        }),
    }
}
