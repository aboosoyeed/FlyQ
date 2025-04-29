use anyhow::{Result};
use flyq_client::client::FlyqClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;

    let topic = "test-topic";
    let payload = b"Hello from FlyQ!".to_vec();

    let ack = client.produce(topic, &payload).await?;

    println!("Produced to partition {}, offset {}", ack.partition, ack.offset);

    Ok(())
}