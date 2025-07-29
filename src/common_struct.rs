use std::{
    io::{Cursor, Read, Seek},
    mem,
    ops::{Deref, DerefMut},
};

use bitflags::bitflags;
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
                    let mut encode_res = array.len().to_be_bytes().to_vec();
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
            let mut encode_res = self.inner.len().to_be_bytes().to_vec();
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
                    let mut encode_res = s.len().to_be_bytes().to_vec();
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
