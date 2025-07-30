#![allow(unused)]

use bincode::Decode;
use bytes::Bytes;
use std::{io::Cursor, mem, path::Path};

use codecrafters_kafka::{
    common_struct::display_bytes,
    response_message::{ResponseHeaderV0, ResponseMessage},
};

#[derive(Debug, Decode)]
struct MyStruct {
    a: Option<[u8; 1024]>,
}

pub fn as_u8(n: u64) -> u8 {
    n as u8
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

    let x = bincode::encode_to_vec(None::<u8>, config).unwrap();
    println!("{:?}", x);

    let x = <Option<u8> as Default>::default();
    println!("{:?}", x);

    let x = -126_i8 as i64;
    dbg!(&x);

    let x = as_u8(0x32_11);
    println!("{}", x);

    let p = Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
    let b = Bytes::from_owner(vec![0x10, 0x01, 0x11]);
    println!("Path: {:?}\nBuffer: {:?}\n{}", p, b, display_bytes(&b));
}
