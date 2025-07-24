#![allow(unused)]

use codecrafters_kafka::encode::Encode;

#[derive(Debug, Encode)]

struct MyStruct1 {
    a: i32,
    b: i32,
}

#[derive(Debug, Encode)]

struct MyStruct2(i32, i32);

fn main() {
    let a = MyStruct2(1, 2);
}
