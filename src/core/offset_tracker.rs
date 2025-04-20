use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct OffsetTracker {
    store: HashMap<String, HashMap<u32, u64>>, // group -> partition -> offset
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn commit(&mut self, group: &str, partition: u32, offset: u64) {
        self.store
            .entry(group.to_string())
            .or_default()
            .insert(partition, offset);
    }

    pub fn fetch(&self, group: &str, partition: u32) -> Option<u64> {
        self.store
            .get(group)
            .and_then(|m| m.get(&partition))
            .copied()
    }
}
