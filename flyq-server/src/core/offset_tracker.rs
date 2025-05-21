use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct OffsetTracker {
    store: HashMap<String, HashMap<u32, u64>>, // group -> partition -> offset
    dirty: HashSet<String>,
    path: PathBuf,
}

impl OffsetTracker {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            store: HashMap::new(),
            dirty: HashSet::new(),
            path: path.into(),
        }
    }

    pub fn commit(&mut self, group: &str, partition: u32, offset: u64) {
        self.store
            .entry(group.to_string())
            .or_default()
            .insert(partition, offset);
        self.dirty.insert(group.to_string());
    }

    pub fn fetch(&self, group: &str, partition: u32) -> Option<u64> {
        self.store
            .get(group)
            .and_then(|m| m.get(&partition))
            .copied()
    }

    pub fn save_to_file(&self) -> Result<(), std::io::Error> {
        // Serialize the internal store (HashMap<String, HashMap<u32, u64>>)
        let json = serde_json::to_string_pretty(&self.store)?;

        // Atomically write to disk (optional: temp file + rename)
        fs::write(&self.path, json)?;

        Ok(())
    }

    pub fn load_from_file(&mut self) -> Result<(), std::io::Error> {
        if self.path.exists() {
            let json = fs::read_to_string(&self.path)?;
            let data: HashMap<String, HashMap<u32, u64>> = serde_json::from_str(&json)?;
            self.store = data;
        }

        Ok(())
    }

    pub fn flush_dirty_offsets(&mut self) -> Result<(), std::io::Error> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        // For simplicity, still flush the entire store. We will improve this later
        self.save_to_file()?; // serializes self.store

        // Clear dirty tracker
        self.dirty.clear();

        Ok(())
    }
}
