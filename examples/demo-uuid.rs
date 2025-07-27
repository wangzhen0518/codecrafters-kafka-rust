use uuid::Uuid;

fn main() {
    let s = "0".repeat(32);
    let uuid = Uuid::parse_str(&s).unwrap();
    println!("{:?}", uuid);
}
