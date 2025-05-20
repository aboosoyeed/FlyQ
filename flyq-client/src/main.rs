use anyhow::Result;
use flyq_client::client::FlyqClient;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;
    
    let topic = "test-topic";
    let payload = b"Hello from FlyQv2!".to_vec();
    
    //let ack = client.produce(topic, &payload).await?;
    //println!("Produced to partition {}, offset {}", ack.partition, ack.offset);
    
    let offset =0;
    match client.consume_with_group(topic, 0, "test_group").await? {
        Some(msg) => {
            println!("Consumed message at offset {}", msg.offset);

            if let Some(key) = msg.message.key {
                println!("Key: {:?}", String::from_utf8_lossy(&key));
            }
            println!("Value: {:?}", String::from_utf8_lossy(&msg.message.value));
            println!("Timestamp: {}", msg.message.timestamp);

            if let Some(headers) = msg.message.headers {
                for (k, v) in headers {
                    println!("Header {}: {:?}", k, String::from_utf8_lossy(&v));
                }
            }
        }
        None => {
            println!("No message found at offset {}", offset);
        }
    }
    
     
    
    Ok(())
}