use std::io::{Cursor, Read};

use bytes::Buf;
use paste::paste;

pub trait Decode {
    fn decode(buffer: &mut Cursor<&[u8]>) -> Self;
}

// 使用宏为所有整数类型实现 Encode
macro_rules! impl_decode_for_integers {
    ($($type:ty),*) => {
        $(
            impl Decode for $type {
                fn decode(buffer: &mut Cursor<&[u8]>) -> Self {
                    // buffer.get_$type() //TODO 减少一次 copy
                    paste! {
                        buffer.[<get_ $type>]()
                    }
                }
            }
        )*
    };
}
// 为所有标准整数类型实现
impl_decode_for_integers!(u8, u16, u32, u64, i8, i16, i32, i64);

impl<T: Decode> Decode for Vec<T> {
    fn decode(buffer: &mut Cursor<&[u8]>) -> Self {
        let mut decode_vec = vec![];
        let length = buffer.get_u8();
        if length > 0 {
            for _ in 0..length {
                let item = T::decode(buffer);
                decode_vec.push(item);
            }
        }
        decode_vec
    }
}

impl Decode for String {
    fn decode(buffer: &mut Cursor<&[u8]>) -> Self {
        let length = buffer.get_i16();
        let mut string_buffer = vec![0; length as usize];
        let _ = buffer.read_exact(&mut string_buffer); //TODO 异常处理
        String::from_utf8(string_buffer).expect("Invalid UTF-8") //TODO 异常处理
    }
}
