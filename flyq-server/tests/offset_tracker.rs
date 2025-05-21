use flyQ::core::log_engine::LogEngine;
use flyq_protocol::Message;
use crate::common::folder_to_use;

mod common;
#[tokio::test]
async fn test_consumer_group_offset_tracking() {

    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(base_dir).await;

    let topic = "logs";
    let group = "analytics";

    // Produce a message
    let msg = Message {
        key: None,
        value: b"event-a".to_vec(),
        timestamp: 1,
        headers: None,
    };

    let (partition, offset) = engine.produce(topic, msg.clone()).expect("produce failed");
    assert_eq!(offset, 0);

    // Consume using group (should default to offset 0)
    let result = engine
        .consume_with_group(topic, partition, group)
        .await
        .expect("consume_with_group failed");

    assert!(result.is_some(), "Expected a message");
    let (consumed_offset, consumed_msg) = result.unwrap();
    assert_eq!(consumed_offset, 0);
    assert_eq!(consumed_msg.value, msg.value);

    // Commit the offset
    engine
        .commit_offset(topic, partition, group, consumed_offset + 1)
        .await
        .expect("commit_offset failed");

    // Consume again from same group (should get nothing)
    let result = engine
        .consume_with_group(topic, partition, group)
        .await
        .expect("consume_with_group after commit failed");

    assert!(result.is_none(), "Expected None after committing past the last offset");
}

#[tokio::test]
async fn test_multiple_consumer_groups_track_offsets_independently() {

    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(base_dir).await;

    let topic = "events";
    let group_a = "group-a";
    let group_b = "group-b";

    // Produce three messages
    for i in 0..3 {
        let msg = Message {
            key: None,
            value: format!("event-{}", i).into_bytes(),
            timestamp: 100 + i,
            headers: None,
        };
        engine.produce(topic, msg).expect("produce failed");
    }

    let partition = 0;

    // Group A consumes and commits offset 1
    let msg_a1 = engine
        .consume_with_group(topic, partition, group_a)
        .await
        .expect("consume group-a #1")
        .unwrap();
    assert_eq!(msg_a1.0, 0);
    engine
        .commit_offset(topic, partition, group_a, msg_a1.0 + 1)
        .await
        .expect("commit group-a #1");

    // Group B consumes and commits offset 1 (still starts from 0)
    let msg_b1 = engine
        .consume_with_group(topic, partition, group_b)
        .await
        .expect("consume group-b #1")
        .unwrap();
    assert_eq!(msg_b1.0, 0);
    engine
        .commit_offset(topic, partition, group_b, msg_b1.0 + 1)
        .await
        .expect("commit group-b #1");

    // Group A consumes and commits offset 2
    let msg_a2 = engine
        .consume_with_group(topic, partition, group_a)
        .await
        .expect("consume group-a #2")
        .unwrap();
    assert_eq!(msg_a2.0, 1);
    engine
        .commit_offset(topic, partition, group_a, msg_a2.0 + 1)
        .await
        .expect("commit group-a #2");

    // Group B consumes and commits offset 2 independently
    let msg_b2 = engine
        .consume_with_group(topic, partition, group_b)
        .await
        .expect("consume group-b #2")
        .unwrap();
    assert_eq!(msg_b2.0, 1);
    engine
        .commit_offset(topic, partition, group_b, msg_b2.0 + 1)
        .await
        .expect("commit group-b #2");

    // Assert final tracked offsets are the same — but were committed independently
    let a_offset = engine
        .offset_tracker
        .lock()
        .await
        .fetch(group_a, partition)
        .expect("group-a offset");

    let b_offset = engine
        .offset_tracker
        .lock()
        .await
        .fetch(group_b, partition)
        .expect("group-b offset");

    assert_eq!(a_offset, 2);
    assert_eq!(b_offset, 2);
}

