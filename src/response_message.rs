use std::io::{self, Cursor};

use crate::{
    api_versions::{execute_api_verions, ApiVersionsResponseBodyV4, API_VERSIONS_API_INFO},
    common_struct::TagBuffer,
    decode::{Decode, DecodeResult},
    describe_topic_partitions::{
        execute_describe_topic_partitions, DescribeTopicPartitionsResponseBodyV0,
        DESCRIBE_TOPIC_PARTITIONS_API_INFO,
    },
    encode::Encode,
    request_message::{RequestBody, RequestHeader, RequestMessage},
};

#[derive(Debug, Encode)]
pub struct ResponseMessage {
    message_size: u32,
    header: ResponseHeader,
    body: ResponseBody,
}

impl ResponseMessage {
    pub fn new(header: ResponseHeader, body: ResponseBody) -> Self {
        ResponseMessage {
            message_size: 0,
            header,
            body,
        }
    }

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

    pub fn decode(buffer: &mut Cursor<&[u8]>, request_api_key: i16) -> DecodeResult<Self> {
        let message_size = u32::decode(buffer)?;
        let header = ResponseHeader::ResponseHeaderV1(ResponseHeaderV1::decode(buffer)?);
        let body = if request_api_key == API_VERSIONS_API_INFO.api_key {
            ResponseBody::ApiVersionsV4(ApiVersionsResponseBodyV4::decode(buffer)?)
        } else if request_api_key == DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key {
            ResponseBody::DescribeTopicPartitionsV0(DescribeTopicPartitionsResponseBodyV0::decode(
                buffer,
            )?)
        } else {
            unimplemented!("Unknown request api key: {}", request_api_key);
        };
        Ok(ResponseMessage {
            message_size,
            header,
            body,
        })
    }
}

#[derive(Debug)]
pub enum ResponseHeader {
    ResponseHeaderV0(ResponseHeaderV0),
    ResponseHeaderV1(ResponseHeaderV1),
}

impl ResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        ResponseHeader::ResponseHeaderV0(ResponseHeaderV0 { correlation_id })
    }

    pub fn new_v1(correlation_id: i32) -> Self {
        ResponseHeader::ResponseHeaderV1(ResponseHeaderV1 {
            correlation_id,
            tag_buffer: TagBuffer::default(),
        })
    }
}

impl Encode for ResponseHeader {
    fn encode(&self) -> Vec<u8> {
        match self {
            ResponseHeader::ResponseHeaderV0(header) => header.encode(),
            ResponseHeader::ResponseHeaderV1(header) => header.encode(),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct ResponseHeaderV0 {
    correlation_id: i32,
}

impl ResponseHeaderV0 {
    pub fn new(correlation_id: i32) -> Self {
        ResponseHeaderV0 { correlation_id }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct ResponseHeaderV1 {
    correlation_id: i32,
    tag_buffer: TagBuffer,
}

impl ResponseHeaderV1 {
    pub fn new(correlation_id: i32, tag_buffer: TagBuffer) -> Self {
        ResponseHeaderV1 {
            correlation_id,
            tag_buffer,
        }
    }
}

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersionsV4(ApiVersionsResponseBodyV4),
    DescribeTopicPartitionsV0(DescribeTopicPartitionsResponseBodyV0),
}

impl Encode for ResponseBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersionsV4(inner) => inner.encode(),
            ResponseBody::DescribeTopicPartitionsV0(inner) => inner.encode(),
        }
    }
}

pub async fn execute_request(request: &RequestMessage) -> io::Result<ResponseMessage> {
    let request_api_key = request.header.request_api_key();
    let create_err = |header, body| {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Unsupport header or body version:
Header: {:?}
Body: {:?}
Support Request Header v2, Describe Topic Partitions V0.",
                header, body
            ),
        ))
    };
    if request_api_key == API_VERSIONS_API_INFO.api_key {
        match (&request.header, &request.body) {
            (RequestHeader::RequestHeaderV2(header), RequestBody::ApiVersionsV4(body)) => {
                Ok(execute_api_verions(header, body))
            }
            (header, body) => create_err(header, body),
        }
    } else if request_api_key == DESCRIBE_TOPIC_PARTITIONS_API_INFO.api_key {
        match (&request.header, &request.body) {
            (
                RequestHeader::RequestHeaderV2(header),
                RequestBody::DescribeTopicPartitionsV0(body),
            ) => Ok(execute_describe_topic_partitions(header, body)),
            (header, body) => create_err(header, body),
        }
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "request_api_key {} has not been implemented",
                request_api_key
            ),
        ))
    }
}
