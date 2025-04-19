use crate::core::constants::{
    DEFAULT_AUTO_CREATE_TOPICS_ENABLE, DEFAULT_INDEX_INTERVAL, DEFAULT_MAX_SEGMENT_BYTES,
    DEFAULT_PARTITION_CNT,
};
use crate::core::message::Message;
use crate::core::storage::Storage;
use crate::core::topic::Topic;
use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

pub struct LogEngine {
    storage: Storage,
    pub topics: HashMap<String, Topic>,
    // optional config knobs:
    max_segment_bytes: u64,
    index_interval: u32,
    auto_create_topic: bool,
}

#[derive(Debug, Error)]
enum EngineError {
    #[error("Topic doesnt exists")]
    NoTopic,
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
                
                if let Some(topic) = Topic::scan_existing(path, self.max_segment_bytes){
                    self.topics.insert(topic.name.clone(), topic);
                }
            }
        }
        Ok(())
    }

    pub fn produce(&mut self, topic: &str, msg: Message) -> std::io::Result<u64> {
        todo!()
    }

    pub fn consume(
        &mut self,
        topic: &str,
        partition: usize,
        offset: u64,
    ) -> std::io::Result<Option<Message>> {
        todo!()
    }
    pub fn create_topic(&mut self,name: impl Into<String>,partition_count: Option<u32>) -> &Topic{
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
}
