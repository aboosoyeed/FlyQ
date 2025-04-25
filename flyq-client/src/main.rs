use anyhow::{Result};
use flyq_client::FlyqClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;
    //client.produce("hello", b"hello world").await?;
    let x = client.consume("hello", 0).await?;
    println!("{:?}", String::from_utf8(x)?);
    Ok(())
}