use anyhow::{Result};
use flyq_client::FlyqClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;
    //client.produce("my-topic", b"hello world").await?;
    let x = client.consume("hello", 1234).await?;
    println!("{}", u64::from_be_bytes(x.try_into().unwrap()));
    Ok(())
}