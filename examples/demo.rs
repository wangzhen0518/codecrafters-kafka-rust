#![allow(unused)]

use bincode::Decode;
use bytes::Bytes;
use std::mem;

use codecrafters_kafka::response_message::{ResponseHeaderV0, ResponseMessage};

#[derive(Debug, Decode)]
struct MyStruct {
    a: Option<[u8; 1024]>,
}

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
    println!("Size of HeaderV0: {}", mem::size_of::<ResponseHeaderV0>());
    println!("Size of Bytes: {}", mem::size_of::<Bytes>());

    let a = MyStruct { a: None };
    let b = MyStruct { a: Some([0; 1024]) };
    println!("Size of a: {}", mem::size_of_val(&a));
    println!("Size of b: {}", mem::size_of_val(&b));

    let x = bincode::encode_to_vec(true, config).unwrap();
    println!("{:?}", x);
}
