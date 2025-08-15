mod common;

use common::folder_to_use;
use flyq_protocol::Message;
use flyQ::core::log_engine::LogEngine;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_watermark_tracking() {
    let base_dir = folder_to_use();
    let engine = Arc::new(Mutex::new(LogEngine::load(&base_dir).await));
    
    let topic = "watermark-test";
    let partition = 0;
    
    // Create topic
    engine.lock().await.create_topic(topic, Some(1));
    
    // Initially, all watermarks should be 0
    let (low, high, log_end) = engine.lock().await.get_watermark(topic, partition).await.unwrap();
    assert_eq!(low, 0);
    assert_eq!(high, 0);
    assert_eq!(log_end, 0);
    
    // Produce some messages
    for i in 0..10 {
        let msg = Message {
            key: None,
            value: format!("Message {}", i).into_bytes(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            headers: None,
        };
        engine.lock().await.produce(topic, msg).await.unwrap();
    }
    
    // Check watermarks after producing
    let (low, high, log_end) = engine.lock().await.get_watermark(topic, partition).await.unwrap();
    assert_eq!(low, 0);  // Low watermark unchanged (no cleanup yet)
    assert_eq!(high, 9); // High watermark is last message offset (0-9)
    assert_eq!(log_end, 10); // Log end offset is next offset to be written
}

#[tokio::test]
async fn test_consumer_lag_calculation() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;
    
    let topic = "lag-test";
    let partition = 0;
    let consumer_group = "test-group";
    
    // Create topic
    engine.create_topic(topic, Some(1));
    
    // Produce 20 messages
    for i in 0..20 {
        let msg = Message {
            key: None,
            value: format!("Message {}", i).into_bytes(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            headers: None,
        };
        engine.produce(topic, msg).await.unwrap();
    }
    
    // Initially, consumer lag should be 19 (committed=0, high=19)
    let (total_lag, partitions) = engine.get_consumer_lag(consumer_group, Some(vec![topic.to_string()])).await.unwrap();
    assert_eq!(total_lag, 19);
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].0, topic);
    assert_eq!(partitions[0].1, partition);
    assert_eq!(partitions[0].2, 0); // committed offset
    assert_eq!(partitions[0].3, 19); // high watermark (last message offset)
    assert_eq!(partitions[0].4, 19); // lag
    
    // Consume and commit offset at position 10
    engine.commit_offset(topic, partition, consumer_group, 10).await.unwrap();
    
    // Now lag should be 9 (19 - 10)
    let (total_lag, partitions) = engine.get_consumer_lag(consumer_group, Some(vec![topic.to_string()])).await.unwrap();
    assert_eq!(total_lag, 9);
    assert_eq!(partitions[0].2, 10); // committed offset
    assert_eq!(partitions[0].3, 19); // high watermark
    assert_eq!(partitions[0].4, 9); // lag
    
    // Commit up to date
    engine.commit_offset(topic, partition, consumer_group, 19).await.unwrap();
    
    // Now lag should be 0
    let (total_lag, _) = engine.get_consumer_lag(consumer_group, Some(vec![topic.to_string()])).await.unwrap();
    assert_eq!(total_lag, 0);
}

#[tokio::test]
async fn test_partition_health() {
    let base_dir = folder_to_use();
    let engine = Arc::new(Mutex::new(LogEngine::load(&base_dir).await));
    
    let topic = "health-test";
    let partition = 0;
    
    // Create topic
    engine.lock().await.create_topic(topic, Some(1));
    
    // Get initial health
    let (segment_count, total_size, low, high, log_end, _) = 
        engine.lock().await.get_partition_health(topic, partition).await.unwrap();
    
    assert_eq!(segment_count, 1); // Should have 1 segment initially
    assert_eq!(total_size, 0); // No data yet
    assert_eq!(low, 0);
    assert_eq!(high, 0);
    assert_eq!(log_end, 0);
    
    // Produce messages to increase size
    for i in 0..100 {
        let msg = Message {
            key: None,
            value: vec![0u8; 1024], // 1KB per message
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            headers: None,
        };
        engine.lock().await.produce(topic, msg).await.unwrap();
    }
    
    // Check health after producing
    let (segment_count, total_size, low, high, log_end, _) = 
        engine.lock().await.get_partition_health(topic, partition).await.unwrap();
    
    assert_eq!(segment_count, 1); // Still 1 segment (not rotated)
    assert!(total_size > 100 * 1024); // Should be > 100KB (includes metadata)
    assert_eq!(low, 0);
    assert_eq!(high, 99);  // High watermark is last message offset
    assert_eq!(log_end, 100); // Log end offset is next offset
}

#[tokio::test]
async fn test_consumer_lag_multiple_topics() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;
    
    let topic1 = "events";
    let topic2 = "logs";
    let consumer_group = "multi-consumer";
    
    // Create topics
    engine.create_topic(topic1, Some(2)); // 2 partitions
    engine.create_topic(topic2, Some(1)); // 1 partition
    
    // Produce to topic1
    for i in 0..30 {
        let msg = Message {
            key: None,
            value: format!("Event {}", i).into_bytes(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            headers: None,
        };
        engine.produce(topic1, msg).await.unwrap();
    }
    
    // Produce to topic2
    for i in 0..20 {
        let msg = Message {
            key: None,
            value: format!("Log {}", i).into_bytes(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            headers: None,
        };
        engine.produce(topic2, msg).await.unwrap();
    }
    
    // Check lag for all topics
    let (total_lag, partitions) = engine.get_consumer_lag(consumer_group, None).await.unwrap();
    
    // Should have 3 partitions total (2 + 1)
    assert_eq!(partitions.len(), 3);
    
    // Total lag should be sum of all partition lags
    let expected_total: u64 = partitions.iter().map(|p| p.4).sum();
    assert_eq!(total_lag, expected_total);
    
    // Commit some offsets
    engine.commit_offset(topic1, 0, consumer_group, 10).await.unwrap();
    engine.commit_offset(topic1, 1, consumer_group, 5).await.unwrap();
    engine.commit_offset(topic2, 0, consumer_group, 15).await.unwrap();
    
    // Check lag again
    let (total_lag, partitions) = engine.get_consumer_lag(consumer_group, None).await.unwrap();
    
    // Verify lag decreased
    assert!(total_lag < expected_total);
    
    // Check specific topic lag
    let (topic1_lag, topic1_partitions) = 
        engine.get_consumer_lag(consumer_group, Some(vec![topic1.to_string()])).await.unwrap();
    assert_eq!(topic1_partitions.len(), 2); // Only topic1's 2 partitions
}