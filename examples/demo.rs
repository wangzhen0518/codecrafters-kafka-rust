use bincode::Options;
use bytes::Bytes;
use std::mem;

use codecrafters_kafka::message::{HeaderV0, ResponseMessage};

fn main() {
    let x = bincode::options()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(&4_u32)
        .unwrap();
    println!("{:?}", x);
    println!(
        "Size of ResponseMessage: {}",
        mem::size_of::<ResponseMessage>()
    );
    println!("Size of HeaderV0: {}", mem::size_of::<HeaderV0>());
    println!("Size of Bytes: {}", mem::size_of::<Bytes>());
}
