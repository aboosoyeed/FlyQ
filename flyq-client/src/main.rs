use anyhow::{Result};
use flyq_client::FlyqClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;
    client.produce("my-topic", b"hello world").await?;
    Ok(())
}