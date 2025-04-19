use std::path::PathBuf;
use crate::core::constants::{DEFAULT_MAX_SEGMENT_BYTES};
use crate::core::partition::Partition;
use crate::core::storage::Storage;

pub struct Topic {
    pub(crate) name: String,
    partitions: Vec<Partition>,
    storage: Storage,
    partition_count: u32,
}

impl Topic {
    pub fn new(name: String, log_engine_storage: &Storage, partition_count: u32) -> Topic {
        let base_path = &log_engine_storage.base_dir;
        let topic_path = base_path.join(Self::get_dir_name(&name));
        let storage = Storage::new(&topic_path);
        let mut partitions: Vec<Partition> = Vec::new();
        for partition_id in 0..partition_count {
            let partition_path =  topic_path.join(format!("partition_{}",partition_id));
            let p =Partition::open( partition_path, partition_id, DEFAULT_MAX_SEGMENT_BYTES).expect("could not create partition");
            partitions.push(p);
        }
        Topic {
            name,
            partitions,
            storage,
            partition_count,
        }
    }
    
    /*
    1. scans a path to check if its a potential topic
    2. if yes load it with partitions 
    */
    pub fn scan_existing(path: PathBuf, max_segment_bytes:u64) -> Option<Topic>{
        let topic_name = path
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|name| name.strip_prefix("topic_"))
            .map(|s| s.to_string());

        let mut partitions: Vec<Partition> = Vec::new();

        if let Some(name) = topic_name {
            let storage = Storage::new(path);
            let entries = storage.scan_base();
            for entry in entries{
                let path = entry.expect("could not open entry").path();
                if let Some(partition) = Partition::scan_existing(path, max_segment_bytes){
                    partitions.push(partition)
                }
            }
            let partition_count = partitions.len() as u32;
            Some(Topic {
                name,
                partitions,
                storage,
                partition_count,
            })
        }else { 
            None
        }
    }

    fn get_dir_name(name: &String) -> String {
        format!("topic_{}", name)
    }
}
