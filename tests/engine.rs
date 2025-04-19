mod common;

use std::fs;

use flyQ::core::log_engine::LogEngine;
use crate::common::folder_to_use;

#[test]
fn test_create_topic_creates_expected_folders_and_metadata() {
    let base_dir = folder_to_use();
    let mut engine = LogEngine::load(&base_dir);
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
