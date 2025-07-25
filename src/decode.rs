use std::{
    fmt::Display,
    io::{Cursor, Read, Seek},
    num::TryFromIntError,
    string::FromUtf8Error,
};

use bytes::Buf;
use paste::paste;

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
impl_decode_for_integers!(u8, u16, u32, u64, i8, i16, i32, i64);

impl<T: Decode> Decode for Vec<T> {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let length = u8::decode(buffer)?;
        let mut decode_vec = vec![];
        assert!(
            length > 0,
            "Vector's length must greater than 0 when decoding"
        );
        for _ in 0..length - 1 {
            let item = T::decode(buffer)?;
            decode_vec.push(item);
        }

        Ok(decode_vec)
    }
}

impl<T: Decode> Decode for Option<Vec<T>> {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let length = u8::decode(buffer)?;
        if length == 0 {
            Ok(None)
        } else {
            buffer.seek_relative(-1).unwrap();
            Ok(Some(<Vec<T> as Decode>::decode(buffer)?))
        }
    }
}

impl Decode for String {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let length = u8::decode(buffer)?;
        assert!(
            length > 0,
            "String's length must greater than 0 when decoding"
        );
        let mut string_buffer = vec![0; (length - 1) as usize];
        buffer.read_exact(&mut string_buffer)?;
        String::from_utf8(string_buffer).map_err(|err| err.into())
    }
}

impl Decode for Option<String> {
    fn decode(buffer: &mut Cursor<&[u8]>) -> DecodeResult<Self> {
        let length = u8::decode(buffer)?;
        if length == 0 {
            Ok(None)
        } else {
            buffer.seek_relative(-1).unwrap();
            Ok(Some(String::decode(buffer)?))
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

impl_decode_other_error_from!(String, &str, FromUtf8Error, TryFromIntError);

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
