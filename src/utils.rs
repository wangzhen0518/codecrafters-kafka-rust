use std::io::Cursor;

use bytes::Buf;
use paste::paste;

pub const U32_SIZE: usize = std::mem::size_of::<i32>();

// 使用宏为所有整数类型实现 Encode
macro_rules! impl_peek_for_integers {
    ($($type:ty),*) => {
        $(
            paste! {
                pub fn [<peek_ $type>](buffer: &mut Cursor<&[u8]>) -> $type {
                    let n = buffer.[<get_ $type>]();
                    // 注意：这里不能用seek_relative，因为Cursor没有这个方法
                    // 所以使用set_position回退
                    buffer.set_position(buffer.position() - std::mem::size_of::<$type>() as u64);
                    n
                }
            }
        )*
    };
}
// 为所有标准整数类型实现
impl_peek_for_integers!(u8, u16, u32, u64, i8, i16, i32, i64);

pub fn config_logger() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .pretty()
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set global subscriber");
}
