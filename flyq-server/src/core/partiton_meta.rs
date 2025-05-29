use std::fs;
use std::fs::File;
use std::io::Write;
// Todo:: investigate if i should use tokio fs
use std::path::Path;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMeta{
    pub low_watermark: u64,
    pub high_watermark: u64,
    pub log_end_offset: u64,
}

impl PartitionMeta {
    pub fn load(path: &Path) -> std::io::Result<Option<PartitionMeta>> {
        
        if path.exists(){
            let file = File::open(path)?;
            let meta: PartitionMeta = serde_json::from_reader(file)?;
            Ok(Some(meta))
        }else{
            Ok(None)
        }
    }

    pub fn save(path: &Path, meta: &PartitionMeta) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("json.tmp");
        {
            let mut tmp_file = File::create(&tmp_path)?;
            serde_json::to_writer_pretty(&mut tmp_file, meta)?;
            tmp_file.flush()?; // make sure data hits disk
        }

        fs::rename(&tmp_path, path)?; // atomic replace
        Ok(())
    }
}