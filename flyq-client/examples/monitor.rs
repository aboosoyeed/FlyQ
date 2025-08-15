use flyq_client::client::FlyqClient;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("FlyQ Monitoring Tool");
    println!("====================\n");
    
    // Connect to FlyQ server
    let mut client = FlyqClient::connect("127.0.0.1:9092").await?;
    println!("Connected to FlyQ server at 127.0.0.1:9092\n");
    
    // Configuration
    let topic = "events";
    let partition = 0;
    let consumer_group = "analytics-group";
    let monitor_interval = Duration::from_secs(5);
    
    loop {
        println!("--- System Status ---");
        
        // Get watermarks
        match client.get_watermarks(topic, partition).await {
            Ok(Some(watermarks)) => {
                println!("Watermarks for {}:{}:", topic, partition);
                println!("  Low watermark:  {}", watermarks.low_watermark);
                println!("  High watermark: {}", watermarks.high_watermark);
                println!("  Log end offset: {}", watermarks.log_end_offset);
                
                let total_messages = watermarks.log_end_offset - watermarks.low_watermark;
                println!("  Total messages: {}", total_messages);
            }
            Ok(None) => println!("No watermark data available"),
            Err(e) => println!("Error getting watermarks: {}", e),
        }
        
        println!();
        
        // Get partition health
        match client.get_partition_health(topic, partition).await {
            Ok(health) => {
                println!("Partition Health for {}:{}:", topic, partition);
                println!("  Segments: {}", health.segment_count);
                println!("  Total size: {} MB", health.total_size_bytes / (1024 * 1024));
                
                if let Some(_last_cleanup) = health.last_cleanup {
                    println!("  Last cleanup: Recently"); // Feature not fully implemented yet
                } else {
                    println!("  Last cleanup: Never");
                }
            }
            Err(e) => println!("Error getting partition health: {}", e),
        }
        
        println!();
        
        // Get consumer lag
        match client.get_consumer_lag(consumer_group, Some(vec![topic.to_string()])).await {
            Ok(lag_response) => {
                println!("Consumer Lag for group '{}':", consumer_group);
                println!("  Total lag: {} messages", lag_response.total_lag);
                
                if !lag_response.partitions.is_empty() {
                    println!("  Per-partition breakdown:");
                    for partition_lag in &lag_response.partitions {
                        println!("    {}:{} - lag: {} (committed: {}, high: {})", 
                            partition_lag.topic,
                            partition_lag.partition,
                            partition_lag.lag,
                            partition_lag.committed_offset,
                            partition_lag.high_watermark
                        );
                    }
                }
                
                // Alert if lag is high
                if lag_response.total_lag > 1000 {
                    println!("  ⚠️  WARNING: Consumer lag is high!");
                } else if lag_response.total_lag > 100 {
                    println!("  ⚠️  NOTICE: Consumer lag is growing");
                } else {
                    println!("  ✅ Consumer lag is healthy");
                }
            }
            Err(e) => println!("Error getting consumer lag: {}", e),
        }
        
        println!("\n--- Waiting {} seconds before next check ---\n", monitor_interval.as_secs());
        sleep(monitor_interval).await;
    }
}