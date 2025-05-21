mod common;

use std::fs;

use flyQ::core::log_engine::LogEngine;
use flyq_protocol::Message;
use crate::common::folder_to_use;

#[tokio::test]
async fn test_create_topic_creates_expected_folders_and_metadata() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;
    let topic_name = "test";
    let partition_count = 2;

    engine.create_topic(topic_name, Some(partition_count));

    let expected_topic_dir = base_dir.join(format!("topic_{}", topic_name));
    assert!(expected_topic_dir.is_dir(), "Expected topic folder was not created");

    let mut partition_dirs = Vec::new();

    let entries = fs::read_dir(&expected_topic_dir)
        .expect("Failed to read topic directory");

    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            partition_dirs.push(path);
        }
    }

    assert_eq!(
        partition_dirs.len(),
        partition_count as usize,
        "Expected {} partitions, found {}",
        partition_count,
        partition_dirs.len()
    );

    for i in 0..partition_count {
        let expected_partition_dir = expected_topic_dir.join(format!("partition_{}", i));
        assert!(
            partition_dirs.contains(&expected_partition_dir),
            "Missing expected partition folder: {:?}",
            expected_partition_dir
        );
    }

    assert_eq!(
        engine.topics.len(),
        1,
        "Expected exactly one topic in engine after creation"
    );
}

#[tokio::test]
async fn produce_creates_topic_and_segment_if_missing() {
    
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;

    let topic_name = "clicks";
    let msg = Message {
        key: None,
        value: b"test payload".to_vec(),
        timestamp: 123456789,
        headers: None,
    };

    // ACT: produce a message
    let (partition_id, offset) = engine.produce(topic_name, msg).expect("produce failed");

    // ASSERT: topic dir created
    let topic_dir = base_dir.join(format!("topic_{}", topic_name));
    assert!(topic_dir.is_dir(), "Topic dir not created");

    // ASSERT: partition dir created
    let partition_dir = topic_dir.join(format!("partition_{}", partition_id));
    assert!(partition_dir.is_dir(), "Partition dir not created");

    // ASSERT: segment file created
    let segment_file = partition_dir.join("segment_00000000000000000000.log");
    assert!(segment_file.is_file(), "Segment file not created");

    // ASSERT: in-memory topic is registered
    assert!(engine.topics.contains_key(topic_name));
    assert_eq!(offset, 0);
}

#[tokio::test]
async fn test_engine_consume_returns_produced_message() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;


    let topic = "events";
    let msg = Message {
        key: None,
        value: b"ping".to_vec(),
        timestamp: 42,
        headers: None,
    };

    // Produce the message
    let (partition_id, offset) = engine.produce(topic, msg.clone()).expect("produce failed");

    // Now consume from the same offset
    let result = engine.consume(topic, partition_id, offset).expect("consume failed");

    assert!(result.is_some(), "Expected message to be returned");
    let returned = result.unwrap();

    assert_eq!(returned.value, msg.value);
    assert_eq!(returned.timestamp, msg.timestamp);
    assert_eq!(returned.key, msg.key);
    assert_eq!(returned.headers, msg.headers);
}

#[tokio::test]
async fn test_consume_past_end_returns_none() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir).await;

    let topic = "orders";
    let msg = Message {
        key: None,
        value: b"hello".to_vec(),
        timestamp: 100,
        headers: None,
    };

    // Produce one message
    let (partition_id, offset) = engine.produce(topic, msg).expect("produce failed");
    assert_eq!(offset, 0);

    // Try consuming one past the last offset
    let result = engine.consume(topic, partition_id, offset + 1)
        .expect("consume failed at offset beyond end");

    assert!(
        result.is_none(),
        "Expected None for offset beyond end, got: {:?}",
        result
    );
}

/*
TODO: add following cases
1. consume() before any message is produced → Ok(None)

2. consume() at the exact end → Ok(None)

3. consume() on corrupted message → Err(Deserialize)

 
*/
