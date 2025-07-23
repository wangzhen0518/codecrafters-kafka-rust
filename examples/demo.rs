use bytes::Bytes;
use std::mem;

use codecrafters_kafka::message::{HeaderV0, ResponseMessage};

fn main() {
    let config = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();
    let x = bincode::encode_to_vec(4_u32, config).unwrap();
    println!("{:?}", x);
    println!(
        "Size of ResponseMessage: {}",
        mem::size_of::<ResponseMessage>()
    );
    println!("Size of HeaderV0: {}", mem::size_of::<HeaderV0>());
    println!("Size of Bytes: {}", mem::size_of::<Bytes>());
}
