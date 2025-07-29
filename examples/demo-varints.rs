use codecrafters_kafka::common_struct::Varint;

fn main() {
    let x = Varint::new(vec![0x96, 0x01]);
    dbg!(x.as_u64());

    let x = Varint::from_u64(150);
    assert_eq!(x, Varint::new(vec![0x96, 0x01]));
    println!("{:#x?}", &x);

    let x = Varint::new(vec![0x82, 0x01]);
    dbg!(x.as_u64());
    dbg!(x.as_i64());

    let x = Varint::new(vec![0x92, 0x01]);
    dbg!(x.as_u64());
    dbg!(x.as_i64());

    let n = 130_u64;
    dbg!(((n << 1) ^ (n >> 63)) as i64);
}
