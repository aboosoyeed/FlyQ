use std::collections::HashMap;
use std::fs;
use std::path::Path;

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

    pub fn save_to_file(&self, path: &Path) -> Result<(), std::io::Error> {
        // Serialize the internal store (HashMap<String, HashMap<u32, u64>>)
        let json = serde_json::to_string_pretty(&self.store)?;

        // Atomically write to disk (optional: temp file + rename)
        fs::write(path, json)?;

        Ok(())
    }

    pub fn load_from_file(&mut self, path: &Path) -> Result<(), std::io::Error> {
        if path.exists() {
            let json = fs::read_to_string(path)?;
            let data: HashMap<String, HashMap<u32, u64>> = serde_json::from_str(&json)?;
            self.store = data;
        }

        Ok(())
    }

}
