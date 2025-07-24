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
impl_encode_for_integers!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl<T: Encode> Encode for &[T] {
    fn encode(&self) -> Vec<u8> {
        if self.len() >= u8::MAX as usize {
            panic!(
                "vector length({}) is greater then u8::MAX({})",
                self.len(),
                u8::MAX
            );
        } else {
            let mut encode_res = vec![(self.len() + 1) as u8];
            for item in self.iter() {
                encode_res.append(&mut item.encode());
            }
            encode_res
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self) -> Vec<u8> {
        self.as_slice().encode()
    }
}

impl<T: Encode> Encode for Option<Vec<T>> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Some(array) => array.encode(),
            None => vec![0_u8],
        }
    }
}

impl Encode for String {
    fn encode(&self) -> Vec<u8> {
        if self.len() >= u8::MAX as usize {
            panic!(
                "string length({}) is greater then i16::MAX({})",
                self.len(),
                i16::MAX
            );
        } else {
            let mut encode_res = vec![(self.len() + 1) as u8];
            encode_res.extend(self.as_bytes());
            encode_res
        }
    }
}

impl Encode for Option<String> {
    fn encode(&self) -> Vec<u8> {
        match self {
            Some(s) => s.encode(),
            None => vec![0_u8],
        }
    }
}
