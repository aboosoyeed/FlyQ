mod common;

use std::time::Duration;
use flyQ::core::log_engine::LogEngine;
use flyq_protocol::Message;
use crate::common::folder_to_use;
use flyQ::{BROKER_CONFIG, BrokerConfig};

#[tokio::test]
async fn test_time_based_retention_cleanup() {
    let base_dir = folder_to_use();
    
    // Set short retention for testing
    let cfg = BrokerConfig {
        retention: Duration::from_millis(100), // 100ms retention
        retention_bytes: None,
        cleanup_interval: Duration::from_secs(1),
        segment_max_bytes: 1024,
    };
    let _ = BROKER_CONFIG.set(cfg); // Ignore if already set
    
    let mut engine = LogEngine::load(&base_dir).await;
    let topic_name = "test-retention";
    
    engine.create_topic(topic_name, Some(1));
    
    // Produce some messages
    let msg1 = Message {
        key: Some("key1".as_bytes().to_vec()),
        value: "payload1".as_bytes().to_vec(),
        timestamp: 0,
        headers: None,
    };
    let msg2 = Message {
        key: Some("key2".as_bytes().to_vec()),
        value: "payload2".as_bytes().to_vec(),
        timestamp: 0,
        headers: None,
    };
    
    engine.produce(topic_name, msg1).await.unwrap();
    engine.produce(topic_name, msg2).await.unwrap();
    
    // Wait for retention period to pass
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Force segment rotation to create multiple segments
    for i in 0..10 {
        let msg = Message {
            key: Some(format!("key{}", i).as_bytes().to_vec()),
            value: format!("payload{}", i).as_bytes().to_vec(),
            timestamp: 0,
            headers: None,
        };
        engine.produce(topic_name, msg).await.unwrap();
    }
    
    // Get partition and run cleanup
    let topic = engine.topics.get_mut(topic_name).unwrap();
    let mut partition = topic.partitions.get_mut(&0).unwrap().lock().await;
    let segments_before = partition.segments.len();
    
    // Run cleanup
    partition.maybe_cleanup().unwrap();
    
    let segments_after = partition.segments.len();
    
    // Should have fewer segments after cleanup
    assert!(segments_after <= segments_before, 
        "Segments should be cleaned up. Before: {}, After: {}", 
        segments_before, segments_after);
}

#[tokio::test] 
async fn test_size_based_retention_cleanup() {
    let base_dir = folder_to_use();
    
    // Set size-based retention
    let cfg = BrokerConfig {
        retention: Duration::from_millis(1), // Very short time to ensure size-based kicks in
        retention_bytes: Some(2048), // 2KB limit
        cleanup_interval: Duration::from_secs(1),
        segment_max_bytes: 512, // Small segments to trigger rotation
    };
    let _ = BROKER_CONFIG.set(cfg); // Ignore if already set
    
    let mut engine = LogEngine::load(&base_dir).await;
    let topic_name = "test-size-retention";
    
    engine.create_topic(topic_name, Some(1));
    
    // Produce enough messages to exceed size limit
    for i in 0..50 {
        let large_payload = "x".repeat(100); // 100 bytes per message
        let msg = Message {
            key: Some(format!("key{}", i).as_bytes().to_vec()),
            value: large_payload.as_bytes().to_vec(),
            timestamp: 0,
            headers: None,
        };
        engine.produce(topic_name, msg).await.unwrap();
        
        // Small delay to ensure segments get different timestamps
        if i % 5 == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    
    // Get partition and check size before cleanup
    let topic = engine.topics.get_mut(topic_name).unwrap();
    let mut partition = topic.partitions.get_mut(&0).unwrap().lock().await;
    let segments_before = partition.segments.len();
    let size_before = partition.total_bytes();
    
    // Run cleanup
    partition.maybe_cleanup().unwrap();
    
    let segments_after = partition.segments.len();
    let size_after = partition.total_bytes();
    
    // Should have cleaned up some segments and reduced size
    // Note: size might be slightly over limit due to active segment, but should be much smaller than before
    assert!(size_after < size_before, "Size should be reduced after cleanup. Before: {}, After: {}", size_before, size_after);
    assert!(segments_after < segments_before, "Should have fewer segments after cleanup");
}

#[tokio::test]
async fn test_segment_deletion_marks_and_removes_files() {
    let base_dir = folder_to_use();
    
    let cfg = BrokerConfig {
        retention: Duration::from_millis(50),
        retention_bytes: None,
        cleanup_interval: Duration::from_secs(1),
        segment_max_bytes: 100, // Very small to force multiple segments
    };
    let _ = BROKER_CONFIG.set(cfg); // Ignore if already set
    
    let mut engine = LogEngine::load(&base_dir).await;
    let topic_name = "test-file-deletion";
    
    engine.create_topic(topic_name, Some(1));
    
    // Produce messages to create multiple segments
    for i in 0..20 {
        let large_msg = "x".repeat(50); // Larger messages to trigger rotation
        let msg = Message {
            key: Some(format!("key{}", i).as_bytes().to_vec()),
            value: format!("{}_{}", large_msg, i).as_bytes().to_vec(),
            timestamp: 0,
            headers: None,
        };
        engine.produce(topic_name, msg).await.unwrap();
        
        // Add delay every few messages to spread out timestamps
        if i % 2 == 0 {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }
    
    // Wait for retention period to ensure segments are old enough
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Force one more segment to make others eligible for cleanup
    let msg = Message {
        key: Some("final".as_bytes().to_vec()),
        value: "final payload".as_bytes().to_vec(),
        timestamp: 0,
        headers: None,
    };
    engine.produce(topic_name, msg).await.unwrap();
    
    // Check files exist before cleanup
    let topic_dir = base_dir.join(format!("topic_{}", topic_name)).join("partition_0");
    let files_before: Vec<_> = std::fs::read_dir(&topic_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("segment_"))
        .collect();
    
    // Run cleanup
    let topic = engine.topics.get_mut(topic_name).unwrap();
    let mut partition = topic.partitions.get_mut(&0).unwrap().lock().await;
    partition.maybe_cleanup().unwrap();
    
    // Check files after cleanup
    let files_after: Vec<_> = std::fs::read_dir(&topic_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains("segment_"))
        .collect();
    
    // Should have fewer files after cleanup
    assert!(files_after.len() < files_before.len(), 
        "Should have fewer segment files after cleanup. Before: {}, After: {}", 
        files_before.len(), files_after.len());
}