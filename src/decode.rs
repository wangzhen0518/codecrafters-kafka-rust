use std::{
    fmt::Display,
    io::{Cursor, Read},
    num, str, string,
};

use bytes::Buf;
use paste::paste;
use uuid::Uuid;

pub use kafka_serde_derive::Decode;

pub trait Decode {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized;
}

// 使用宏为所有整数类型实现 Encode
macro_rules! impl_decode_for_integers {
    ($($type:ty),*) => {
        $(
            impl Decode for $type {
                fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
                    if buffer.remaining() < std::mem::size_of::<$type>() {
                        Err(DecodeError::Incomplete(None))
                    } else {
                        paste! { Ok(buffer.[<get_ $type>]()) }
                    }
                }
            }
        )*
    };
}
// 为所有标准整数类型实现
impl_decode_for_integers!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl Decode for bool {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        match u8::decode(buffer)? {
            0 => Ok(false),
            1 => Ok(true),
            x => Err(DecodeError::Other(
                format!("Found {} when decoding bool", x).into(),
            )),
        }
    }
}

#[derive(Debug)]
pub enum DecodeError {
    Incomplete(Option<Box<dyn std::error::Error + Send + Sync>>),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Incomplete(err) => {
                "stream ended early".fmt(f)?;
                match err {
                    Some(err) => err.fmt(f),
                    None => Ok(()),
                }
            }
            DecodeError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for DecodeError {}

macro_rules! impl_decode_other_error_from {
    ($($type:ty),*) => {
        $(
            impl From<$type> for DecodeError {
                fn from(value: $type) -> Self {
                    DecodeError::Other(value.into())
                }
            }
        )*
    };
}

impl_decode_other_error_from!(
    &str,
    str::Utf8Error,
    String,
    string::FromUtf8Error,
    num::TryFromIntError,
    uuid::Error
);

macro_rules! impl_decode_imcomplete_error_from {
    ($($type:ty),*) => {
        $(
            impl From<$type> for DecodeError {
                fn from(value: $type) -> Self {
                    DecodeError::Incomplete(Some(value.into()))
                }
            }
        )*
    };
}

impl_decode_imcomplete_error_from!(std::io::Error);

pub type DecodeResult<T> = Result<T, DecodeError>;

impl Decode for Uuid {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self>
    where
        Self: Sized,
    {
        let mut uuid_buffer = [0_u8; 16];
        buffer.read_exact(&mut uuid_buffer)?;
        let uuid = Uuid::from_bytes(uuid_buffer);
        Ok(uuid)
    }
}
