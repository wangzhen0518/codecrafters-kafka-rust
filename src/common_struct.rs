use std::{
    cmp::min,
    io::{Cursor, Read, Seek},
    mem,
    ops::{Deref, DerefMut},
};

use bitflags::bitflags;
use bytes::Buf;
use uuid::Uuid;

use crate::{
    decode::{Decode, DecodeError, DecodeResult},
    describe_topic_partitions::RepicaNode,
    encode::Encode,
};

const VARINTS_MASK: u8 = 0x7f;
const PAY_LOAD_BIT_NUM: u8 = 7;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct VarInt {
    bytes: Vec<u8>,
}

#[inline(always)]
fn zigzag_encode_64bit(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

#[inline(always)]
fn zigzag_decode_64bit(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 0x01) as i64))
}

impl VarInt {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { bytes: inner }
    }

    pub fn from_u64(mut n: u64) -> Self {
        let mut bytes = vec![];
        let mut byte = n as u8 & VARINTS_MASK;
        n >>= PAY_LOAD_BIT_NUM;
        while n > 0 {
            byte |= !VARINTS_MASK;
            bytes.push(byte);
            byte = n as u8 & VARINTS_MASK;
            n >>= PAY_LOAD_BIT_NUM;
        }
        bytes.push(byte);
        VarInt::new(bytes)
    }

    pub fn from_i64(n: i64) -> Self {
        let n = zigzag_encode_64bit(n);
        VarInt::from_u64(n)
    }

    pub fn as_u64(&self) -> u64 {
        let mut n = 0;
        for byte in self.bytes.iter().rev() {
            let payload = byte & VARINTS_MASK;
            n = n << PAY_LOAD_BIT_NUM | (payload as u64);
        }
        n
    }

    pub fn as_i64(&self) -> i64 {
        let n = self.as_u64();
        zigzag_decode_64bit(n)
    }

    pub fn as_bytes(&self) -> &Vec<u8> {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Encode for VarInt {
    fn encode(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

impl Decode for VarInt {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let mut bytes = vec![];
        let mut byte = u8::decode(buffer)?;
        while byte >> PAY_LOAD_BIT_NUM == 1 {
            bytes.push(byte);
            byte = u8::decode(buffer)?;
        }
        bytes.push(byte);
        Ok(VarInt::new(bytes))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct VarLong {
    bytes: Vec<u8>,
}

#[inline(always)]
fn zigzag_encode_128bit(n: i128) -> u128 {
    ((n << 1) ^ (n >> 127)) as u128
}

#[inline(always)]
fn zigzag_decode_128bit(n: u128) -> i128 {
    ((n >> 1) as i128) ^ (-((n & 0x01) as i128))
}

impl VarLong {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { bytes: inner }
    }

    pub fn from_u128(mut n: u128) -> Self {
        let mut bytes = vec![];
        let mut byte = n as u8 & VARINTS_MASK;
        n >>= PAY_LOAD_BIT_NUM;
        while n > 0 {
            byte |= !VARINTS_MASK;
            bytes.push(byte);
            byte = n as u8 & VARINTS_MASK;
            n >>= PAY_LOAD_BIT_NUM;
        }
        bytes.push(byte);
        VarLong::new(bytes)
    }

    pub fn from_i128(n: i128) -> Self {
        let n = zigzag_encode_128bit(n);
        VarLong::from_u128(n)
    }

    pub fn as_u128(&self) -> u128 {
        let mut n = 0;
        for byte in self.bytes.iter().rev() {
            let payload = byte & VARINTS_MASK;
            n = n << PAY_LOAD_BIT_NUM | (payload as u128);
        }
        n
    }

    pub fn as_i128(&self) -> i128 {
        let n = self.as_u128();
        zigzag_decode_128bit(n)
    }

    pub fn as_bytes(&self) -> &Vec<u8> {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Encode for VarLong {
    fn encode(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

impl Decode for VarLong {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let mut bytes = vec![];
        let mut byte = u8::decode(buffer)?;
        while byte >> PAY_LOAD_BIT_NUM == 1 {
            bytes.push(byte);
            byte = u8::decode(buffer)?;
        }
        bytes.push(byte);
        Ok(VarLong::new(bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Array<T> {
    inner: Option<Vec<T>>,
}

impl<T> Array<T> {
    pub fn new(inner: Option<Vec<T>>) -> Self {
        Self { inner }
    }
}

impl<T: Encode> Encode for Array<T> {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0xff; 4],
            Some(array) => {
                if array.len() >= i32::MAX as usize {
                    panic!(
                        "Array length({}) is greater then i32::MAX({})",
                        array.len(),
                        i32::MAX
                    );
                } else {
                    let mut encode_res = (array.len() as i32).to_be_bytes().to_vec();
                    for item in array.iter() {
                        encode_res.append(&mut item.encode());
                    }
                    encode_res
                }
            }
        }
    }
}

impl<T: Decode> Decode for Array<T> {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = i32::decode(buffer)?;
        let inner = if length >= 0 {
            let mut decode_res = vec![];
            for _ in 0..length {
                let item = T::decode(buffer)?;
                decode_res.push(item);
            }
            Some(decode_res)
        } else {
            None
        };
        Ok(Array::new(inner))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompactArray<T> {
    inner: Option<Vec<T>>,
}

impl<T> CompactArray<T> {
    pub fn new(inner: Option<Vec<T>>) -> Self {
        Self { inner }
    }
}

impl<T: Encode> Encode for CompactArray<T> {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0x00],
            Some(array) => {
                let mut encode_res = VarInt::from_u64((array.len() + 1) as u64).into_bytes();
                for item in array.iter() {
                    encode_res.append(&mut item.encode());
                }
                encode_res
            }
        }
    }
}

impl<T: Decode> Decode for CompactArray<T> {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        let inner = if length > 0 {
            let mut decode_res = vec![];
            for _ in 0..length - 1 {
                let item = T::decode(buffer)?;
                decode_res.push(item);
            }
            Some(decode_res)
        } else {
            None
        };
        Ok(CompactArray::new(inner))
    }
}

macro_rules! impl_deref_for_array {
    ($($type:tt<$gen:tt>),*) => {
        $(
            impl<$gen> Deref for $type<$gen> {
                type Target = Option<Vec<$gen>>;
                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl<$gen> DerefMut for $type<$gen> {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.inner
                }
            }
        )*
    };
}
impl_deref_for_array!(Array<T>, CompactArray<T>);

macro_rules! impl_default_for_array {
    ($($type:tt<$gen:tt>),*) => {
        $(
            impl<$gen> Default for $type<$gen> {
                fn default() -> Self {
                    Self::new(None)
                }
            }
        )*
    };
}
impl_default_for_array!(Array<T>, CompactArray<T>);

macro_rules! impl_empty_for_array {
    ($($type:tt<$gen:tt>),*) => {
        $(
            impl<$gen> $type<$gen> {
                pub fn empty() -> Self {
                    Self::new(Some(vec![]))
                }
            }
        )*
    };
}
impl_empty_for_array!(Array<T>, CompactArray<T>);

macro_rules! impl_inner_for_array {
    ($($type:tt<$gen:tt>),*) => {
        $(
            impl<$gen> $type<$gen> {
                pub fn get_inner(&self) -> &Option<Vec<T>> {
                    &self.inner
                }
            }
        )*
    };
}
impl_inner_for_array!(Array<T>, CompactArray<T>);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct KafkaString {
    inner: String,
}

impl KafkaString {
    pub fn new(inner: String) -> Self {
        Self { inner }
    }
}

impl Encode for KafkaString {
    fn encode(&self) -> Vec<u8> {
        if self.inner.len() > i16::MAX as usize {
            panic!(
                "KafkaString length({}) is bigger than i16::MAX({})",
                self.inner.len(),
                i16::MAX
            );
        } else {
            let mut encode_res = (self.inner.len() as i16).to_be_bytes().to_vec();
            encode_res.extend(self.inner.as_bytes());
            encode_res
        }
    }
}

impl Decode for KafkaString {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = i16::decode(buffer)?;
        assert!(
            length >= 0,
            "KafkaString's length cannot smaller than 0 when decoding"
        );
        let mut string_buffer = vec![0; length as usize]; //TODO 是否需要预先置零
        buffer.read_exact(&mut string_buffer)?;
        let s = String::from_utf8(string_buffer)?;
        Ok(KafkaString::new(s))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompactString {
    inner: String,
}

impl CompactString {
    pub fn new(inner: String) -> Self {
        Self { inner }
    }
}

impl Encode for CompactString {
    fn encode(&self) -> Vec<u8> {
        let mut encode_res = VarInt::from_u64((self.inner.len() + 1) as u64).into_bytes();
        encode_res.extend(self.inner.as_bytes());
        encode_res
    }
}

impl Decode for CompactString {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        assert!(
            length > 0,
            "CompactString's length must bigger than 0 when decoding"
        );
        let mut string_buffer = vec![0; (length - 1) as usize]; //TODO 是否需要预先置零
        buffer.read_exact(&mut string_buffer)?;
        let s = String::from_utf8(string_buffer)?;
        Ok(CompactString::new(s))
    }
}

macro_rules! impl_deref_for_string {
    ($($type:ty),*) => {
        $(
            impl Deref for $type {
                type Target = String;
                fn deref(&self) -> &Self::Target {
                    &self.inner
                }
            }

            impl DerefMut for $type {
                fn deref_mut(&mut self) -> &mut Self::Target {
                    &mut self.inner
                }
            }
        )*
    };
}
impl_deref_for_string!(KafkaString, CompactString);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NullableString {
    inner: Option<String>,
}

impl NullableString {
    pub fn new(inner: Option<String>) -> Self {
        Self { inner }
    }
}

impl Encode for NullableString {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0xff; mem::size_of::<i16>()],
            Some(s) => {
                if s.len() > i16::MAX as usize {
                    panic!(
                        "NullableString length({}) is bigger than i16::MAX({})",
                        s.len(),
                        i16::MAX
                    );
                } else {
                    let mut encode_res = (s.len() as i16).to_be_bytes().to_vec();
                    encode_res.extend(s.as_bytes());
                    encode_res
                }
            }
        }
    }
}

impl Decode for NullableString {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = i16::decode(buffer)?;
        let inner = if length >= 0 {
            let mut string_buffer = vec![0; length as usize]; //TODO 是否需要预先置零
            buffer.read_exact(&mut string_buffer)?;
            let s = String::from_utf8(string_buffer)?;
            Some(s)
        } else {
            None
        };
        Ok(NullableString::new(inner))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompactNullableString {
    inner: Option<String>,
}

impl CompactNullableString {
    pub fn new(inner: Option<String>) -> Self {
        Self { inner }
    }
}

impl Encode for CompactNullableString {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0x00],
            Some(s) => {
                let mut encode_res = VarInt::from_u64((s.len() + 1) as u64).into_bytes();
                encode_res.extend(s.as_bytes());
                encode_res
            }
        }
    }
}

impl Decode for CompactNullableString {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        let inner = if length > 0 {
            let mut string_buffer = vec![0; (length - 1) as usize]; //TODO 是否需要预先置零
            buffer.read_exact(&mut string_buffer)?;
            let s = String::from_utf8(string_buffer)?;
            Some(s)
        } else {
            None
        };
        Ok(CompactNullableString::new(inner))
    }
}

#[derive(Debug, Clone, Default)]
pub struct KafkaBytes {
    inner: Vec<u8>,
}

impl KafkaBytes {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { inner }
    }
}

impl Encode for KafkaBytes {
    fn encode(&self) -> Vec<u8> {
        if self.inner.len() >= i32::MAX as usize {
            panic!(
                "KafkaBytes length({}) is greater then i32::MAX({})",
                self.inner.len(),
                i32::MAX
            );
        } else {
            let mut encode_res = (self.inner.len() as i32).to_be_bytes().to_vec();
            encode_res.append(&mut self.inner.clone());
            encode_res
        }
    }
}

impl Decode for KafkaBytes {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = i32::decode(buffer)?;
        let mut bytes = vec![0_u8; length as usize];
        buffer.read_exact(&mut bytes)?;
        Ok(KafkaBytes::new(bytes))
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactBytes {
    inner: Vec<u8>,
}

impl CompactBytes {
    pub fn new(inner: Vec<u8>) -> Self {
        Self { inner }
    }
}

impl Encode for CompactBytes {
    fn encode(&self) -> Vec<u8> {
        let mut encode_res = VarInt::from_u64((self.inner.len() + 1) as u64).into_bytes();
        encode_res.extend_from_slice(&self.inner);
        encode_res
    }
}

impl Decode for CompactBytes {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        assert!(
            length > 0,
            "CompactBytes's length must bigger than 0 when decoding"
        );
        let mut inner = vec![0; (length - 1) as usize]; //TODO 是否需要预先置零
        buffer.read_exact(&mut inner)?;
        Ok(CompactBytes::new(inner))
    }
}

#[derive(Debug, Clone, Default)]
pub struct NullableBytes {
    inner: Option<Vec<u8>>,
}

impl NullableBytes {
    pub fn new(inner: Option<Vec<u8>>) -> Self {
        Self { inner }
    }
}

impl Encode for NullableBytes {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0xff; mem::size_of::<i32>()],
            Some(array) => {
                if array.len() > i32::MAX as usize {
                    panic!(
                        "NullableBytes length({}) is bigger than i32::MAX({})",
                        array.len(),
                        i32::MAX
                    );
                } else {
                    let mut encode_res = (array.len() as i32).to_be_bytes().to_vec();
                    encode_res.extend_from_slice(array);
                    encode_res
                }
            }
        }
    }
}

impl Decode for NullableBytes {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = i32::decode(buffer)?;
        let inner = if length >= 0 {
            let mut inner = vec![0; length as usize]; //TODO 是否需要预先置零
            buffer.read_exact(&mut inner)?;
            Some(inner)
        } else {
            None
        };
        Ok(NullableBytes::new(inner))
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactNullableBytes {
    inner: Option<Vec<u8>>,
}

impl CompactNullableBytes {
    pub fn new(inner: Option<Vec<u8>>) -> Self {
        Self { inner }
    }
}

impl Encode for CompactNullableBytes {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0x00],
            Some(array) => {
                let mut encode_res = VarInt::from_u64((array.len() + 1) as u64).into_bytes();
                encode_res.extend_from_slice(array);
                encode_res
            }
        }
    }
}

impl Decode for CompactNullableBytes {
    fn decode(buffer: &mut std::io::Cursor<&[u8]>) -> crate::decode::DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        let inner = if length > 0 {
            let mut inner = vec![0; (length - 1) as usize]; //TODO 是否需要预先置零
            buffer.read_exact(&mut inner)?;
            Some(inner)
        } else {
            None
        };
        Ok(CompactNullableBytes::new(inner))
    }
}

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct TagBuffer {
    fields: CompactArray<TagSection>,
}

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct TagSection {
    tag: u8,
    data: CompactArray<u8>,
}

impl TagBuffer {
    pub fn new(fields: CompactArray<TagSection>) -> Self {
        Self { fields }
    }
}

impl TagSection {
    pub fn new(tag: u8, data: Option<Vec<u8>>) -> Self {
        Self {
            tag,
            data: CompactArray::new(data),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
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

pub struct RecordType;

impl RecordType {
    pub const TOPIC_RECORD: i8 = 0x02;
    pub const PARITION_RECORD: i8 = 0x03;
    pub const FEATURE_LEVEL_RECORD: i8 = 0x0c;
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Record {
    pub length: VarInt, // signed
    pub attributes: i8,
    pub timestamp_delta: VarLong,
    pub offset_delta: VarInt,
    pub key: RecordKey,
    pub value: RecordValue,
    pub headers_array_count: CompactArray<RecordHeader>,
}

impl Record {
    pub fn get_value(&self) -> &RecordValue {
        &self.value
    }
}

#[derive(Debug, Clone)]
pub struct RecordKey {
    inner: Option<Vec<u8>>,
}

impl RecordKey {
    pub fn new(inner: Option<Vec<u8>>) -> Self {
        Self { inner }
    }

    pub fn get_inner(&self) -> &Option<Vec<u8>> {
        &self.inner
    }
}

impl Encode for RecordKey {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0x01],
            Some(array) => {
                let mut encode_res = VarInt::from_i64(array.len() as i64).into_bytes();
                encode_res.extend_from_slice(array);
                encode_res
            }
        }
    }
}

impl Decode for RecordKey {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_i64();
        let inner = if length >= 0 {
            let mut decode_res = vec![0_u8; length as usize];
            buffer.read_exact(&mut decode_res)?;
            Some(decode_res)
        } else {
            None
        };
        Ok(RecordKey::new(inner))
    }
}

#[derive(Debug, Clone)]
pub enum RecordValue {
    Topic(TopicRecord),
    Partition(ParitionRecord),
    FeatureLevel(FeatureLevelRecord),
    Unknown(Vec<u8>),
}

impl RecordValue {
    pub fn is_unknown(&self) -> bool {
        matches!(self, RecordValue::Unknown(_))
    }
}

impl Encode for RecordValue {
    fn encode(&self) -> Vec<u8> {
        match &self {
            RecordValue::Topic(record) => {
                let mut record_encode = record.encode();
                let mut encode_res = VarInt::from_i64(record_encode.len() as i64).into_bytes();
                encode_res.append(&mut record_encode);
                encode_res
            }
            RecordValue::Partition(record) => {
                let mut record_encode = record.encode();
                let mut encode_res = VarInt::from_i64(record_encode.len() as i64).into_bytes();
                encode_res.append(&mut record_encode);
                encode_res
            }
            RecordValue::FeatureLevel(record) => {
                let mut record_encode = record.encode();
                let mut encode_res = VarInt::from_i64(record_encode.len() as i64).into_bytes();
                encode_res.append(&mut record_encode);
                encode_res
            }
            RecordValue::Unknown(record_encode) => {
                let mut encode_res = VarInt::from_i64(record_encode.len() as i64).into_bytes();
                encode_res.extend_from_slice(record_encode);
                encode_res
            }
        }
    }
}

impl Decode for RecordValue {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let value_length = VarInt::decode(buffer)?;
        let _frame_version = i8::decode(buffer)?;
        let record_type = i8::decode(buffer)?;

        buffer.seek_relative(-2).expect("Failed to seek");
        let position = buffer.position();

        let record_value = match parse_known_record(record_type, buffer) {
            Ok(record_value) => record_value,
            Err(err) => {
                tracing::error!("{}", err);
                buffer.set_position(position);
                let mut record_encode = vec![0x00; value_length.as_i64() as usize];
                buffer.read_exact(&mut record_encode)?;
                RecordValue::Unknown(record_encode)
            }
        };
        Ok(record_value)
    }
}

fn parse_known_record(record_type: i8, buffer: &mut Cursor<&[u8]>) -> DecodeResult<RecordValue> {
    match record_type {
        RecordType::TOPIC_RECORD => Ok(RecordValue::Topic(TopicRecord::decode(buffer)?)),
        RecordType::PARITION_RECORD => Ok(RecordValue::Partition(ParitionRecord::decode(buffer)?)),
        RecordType::FEATURE_LEVEL_RECORD => Ok(RecordValue::FeatureLevel(
            FeatureLevelRecord::decode(buffer)?,
        )),
        record_type => Err(DecodeError::Other(
            format!("Unknown record type: {}", record_type).into(),
        )),
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
    pub topic_id: Uuid,
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct RecordHeader {
    key: CompactString,
    value: CompactArray<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct CompactRecords {
    inner: Option<Vec<RecordBatch>>,
}

impl CompactRecords {
    pub fn new(inner: Option<Vec<RecordBatch>>) -> Self {
        Self { inner }
    }

    pub fn empty() -> Self {
        Self {
            inner: Some(vec![]),
        }
    }
}

impl Encode for CompactRecords {
    fn encode(&self) -> Vec<u8> {
        match &self.inner {
            None => vec![0x00],
            Some(array) => {
                let mut records_encode: Vec<u8> = array
                    .iter()
                    .flat_map(|record_batch| record_batch.encode())
                    .collect();
                let mut encode_res =
                    VarInt::from_u64((records_encode.len() + 1) as u64).into_bytes();
                encode_res.append(&mut records_encode);
                encode_res
            }
        }
    }
}

impl Decode for CompactRecords {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let length = VarInt::decode(buffer)?.as_u64();
        let inner = if length > 0 {
            let mut inner_buffer = vec![0x00; (length - 1) as usize];
            buffer.read_exact(&mut inner_buffer)?;
            let mut inner_buffer = Cursor::new(inner_buffer.as_slice());
            let mut record_batches = vec![];
            while inner_buffer.has_remaining() {
                record_batches.push(RecordBatch::decode(&mut inner_buffer)?);
            }
            Some(record_batches)
        } else {
            None
        };
        Ok(CompactRecords::new(inner))
    }
}

pub fn display_bytes(bytes: &[u8]) -> String {
    let mut s = String::new();

    let mut col = 0x00;
    let mut row = 0x00;

    s.push_str("   ");
    for i in 0..min(bytes.len(), 16) {
        s = format!("{}{:02x} ", s, i);
    }
    s = format!("{}\n", s);

    for byte in bytes {
        if col == 0 {
            s = format!("{}{:02x} ", s, row);
            row += 0x10;
        }
        s = format!("{}{:02x} ", s, byte);
        col += 1;
        if col == 0x10 {
            col = 0x00;
            s = format!("{}\n", s);
        }
    }
    if col != 0x00 {
        s = format!("{}\n", s);
    }
    s
}
