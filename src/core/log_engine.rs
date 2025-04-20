use crate::core::constants::{
    DEFAULT_AUTO_CREATE_TOPICS_ENABLE, DEFAULT_INDEX_INTERVAL, DEFAULT_MAX_SEGMENT_BYTES,
    DEFAULT_PARTITION_CNT,
};
use crate::core::error::{DeserializeError, EngineError};
use crate::core::message::Message;
use crate::core::offset_tracker::OffsetTracker;
use crate::core::storage::Storage;
use crate::core::topic::Topic;
use std::collections::HashMap;
use std::path::Path;

pub struct LogEngine {
    storage: Storage,
    pub topics: HashMap<String, Topic>,
    // optional config knobs:
    max_segment_bytes: u64,
    index_interval: u32,
    auto_create_topic: bool,
    pub offset_tracker: OffsetTracker,
}

impl LogEngine {
    pub fn load<P: AsRef<Path>>(base_dir: P) -> LogEngine {
        let storage = Storage::new(base_dir);
        let mut engine = LogEngine {
            storage,
            topics: HashMap::new(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            index_interval: DEFAULT_INDEX_INTERVAL,
            auto_create_topic: DEFAULT_AUTO_CREATE_TOPICS_ENABLE,
            offset_tracker: OffsetTracker::new(),
        };
        engine
            .scan_topics()
            .expect("Failed to scan topic directories");
        engine
    }

    fn scan_topics(&mut self) -> std::io::Result<()> {
        let entries = self.storage.scan_base();
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                if let Some(topic) = Topic::scan_existing(path, self.max_segment_bytes) {
                    self.topics.insert(topic.name.clone(), topic);
                }
            }
        }
        Ok(())
    }

    pub fn produce(&mut self, topic_name: &str, msg: Message) -> std::io::Result<(u32, u64)> {
        if !self.topics.contains_key(topic_name) {
            self.ensure_topic(topic_name)
                .expect("topic creation failed");
        }
        let topic = self
            .topics
            .get_mut(topic_name)
            .expect("topic should exist now");
        topic.produce(msg)
    }

    pub fn consume(
        &mut self,
        topic_name: &str,
        partition_id: u32,
        offset: u64,
    ) -> Result<Option<Message>, EngineError> {
        tracing::debug!(topic = %topic_name, partition_id, offset, "consume request");
        let topic = self
            .topics
            .get_mut(topic_name)
            .ok_or(EngineError::NoTopic)?;
        let partition = topic
            .partitions
            .get_mut(&partition_id)
            .ok_or(EngineError::NoPartition)?;
        let mut stream = match partition.stream_from_offset(offset) {
            Ok(s) => s,
            Err(DeserializeError::OffsetNotFound(_)) => return Ok(None), // 👈 graceful EOF
            Err(e) => return Err(e.into()), // 👈 other deserialization errors
        };

        match stream.next() {
            Some(Ok((_offset, msg))) => Ok(Some(msg)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }
    pub fn create_topic(
        &mut self,
        name: impl Into<String>,
        partition_count: Option<u32>,
    ) -> &Topic {
        let name = name.into();
        let topic = Topic::new(
            name.clone(),
            &self.storage,
            partition_count.unwrap_or(DEFAULT_PARTITION_CNT),
        );
        self.topics.insert(name.clone(), topic);
        self.topics.get(&name).unwrap()
    }

    fn ensure_topic(&mut self, name: &str) -> Result<&Topic, EngineError> {
        if self.topics.contains_key(name) {
            return Ok(self.topics.get(name).unwrap());
        }

        if self.auto_create_topic {
            Ok(self.create_topic(name, None))
        } else {
            Err(EngineError::NoTopic)
        }
    }

    fn high_watermark(&self, topic: &str, partition_id: u32) -> Result<u64, EngineError> {
        let topic = self.topics.get(topic).ok_or(EngineError::NoTopic)?;
        let partition = topic
            .partitions
            .get(&partition_id)
            .ok_or(EngineError::NoPartition)?;
        Ok(partition.high_watermark())
    }

    pub fn consume_with_group(
        &mut self,
        topic: &str,
        partition: u32,
        group: &str,
    ) -> Result<Option<(u64, Message)>, EngineError> {
        let offset = self
            .offset_tracker
            .fetch(group, partition)
            .unwrap_or(0); // default to beginning

        let message = self.consume(topic, partition, offset)?;
        Ok(message.map(|msg| (offset, msg)))
    }

    pub fn commit_offset(
        &mut self,
        topic: &str,
        partition: u32,
        group: &str,
        offset: u64,
    ) -> Result<(), EngineError> {
        if !self.topics.contains_key(topic) {
            return Err(EngineError::NoTopic);
        }
        self.offset_tracker.commit(group, partition, offset);
        Ok(())
    }
}
