// cargo run produce ~/personal/data "hello world"
// cargo run consume  ~/personal/data 0

use flyQ::core::log_engine::LogEngine;
use flyQ::core::message::Message;

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    //let command = "consume";
    //let consume_offset = 2;
    let produce_message = "hello";

    let mut engine = LogEngine::load("/home/aboosoyeed/personal/data");
    for item in engine.topics.iter() {
        println!("{}", item.0)
    }

    let msg = Message {
        key: None,
        value: Vec::from(produce_message),
        timestamp: 0,
        headers: None,
    };

    engine.produce("abc", msg);

    Ok(())
}
