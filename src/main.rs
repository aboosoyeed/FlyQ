use std::path::PathBuf;
use crate::core::partition::Partition;

mod core;

// cargo run produce ~/personal/data "hello world"
// cargo run consume  ~/personal/data 0


fn main() -> std::io::Result<()> {

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();


    let command = "consume";
    let log_dir = PathBuf::from("/home/aboosoyeed/personal/data");
    let consume_offset = 2;
    let produce_message="hello";

    match command {
        "produce" => {

            let mut partition = Partition::open(log_dir, 0, 1024)?;

            let msg = core::message::Message {
                key: None,
                value: Vec::from(produce_message),
                timestamp: 0,
                headers: None,
            };

            let offset = partition.append(&msg)?;


            println!("Produced at offset {}", offset);
        }

        "consume" => {
            let mut partition = Partition::open(log_dir, 0, 1024)?;
            let messages = partition.read_from_offset(consume_offset).unwrap();
            for msg in messages.iter(){
                println!("{:?}", String::from_utf8(msg.value.clone()).unwrap())
            }

        },
        &_ => todo!()

    }

    Ok(())
}
