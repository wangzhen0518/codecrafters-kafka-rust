use uuid::Uuid;

pub use kafka_serde_derive::Encode;

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

// 使用宏为所有整数类型实现 Encode
macro_rules! impl_encode_for_integers {
    ($($type:ty),*) => {
        $(
            impl Encode for $type {
                fn encode(&self) -> Vec<u8> {
                    self.to_be_bytes().to_vec() //TODO 减少一次 copy
                }
            }
        )*
    };
}
// 为所有标准整数类型实现
impl_encode_for_integers!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, isize, i128);

impl Encode for bool {
    fn encode(&self) -> Vec<u8> {
        u8::from(*self).encode()
    }
}

impl Encode for Uuid {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}
