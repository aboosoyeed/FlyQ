use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use xxhash_rust::xxh3::xxh3_64;
use flyq_protocol::message::Message;
use crate::core::partition::Partition;
use crate::core::storage::Storage;
pub type SharedPartition = Arc<Mutex<Partition>>;
pub struct Topic {
    pub(crate) name: String,
    pub partitions: HashMap<u32,Arc<Mutex<Partition>>>,
    storage: Storage,
    partition_count: u32,
    next_partition:u32, // used for partition tracking in round robin allocation
}

impl Topic {
    pub fn new(name: String, log_engine_storage: &Storage, partition_count: u32, max_segment_bytes:u64) -> Topic {
        let base_path = &log_engine_storage.base_dir;
        let topic_path = base_path.join(Self::get_dir_name(&name));
        let storage = Storage::new(&topic_path);
        let mut partitions: HashMap<u32,SharedPartition> =HashMap::new();
        for partition_id in 0..partition_count {
            let partition_path =  topic_path.join(format!("partition_{}",partition_id));
            let p =Partition::open( partition_path, partition_id, max_segment_bytes).expect("could not create partition");
            let shared_partition = Arc::new(Mutex::new(p));
            partitions.insert(partition_id, shared_partition);
        }
        Topic {
            name,
            partitions,
            storage,
            partition_count,
            next_partition:0
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

        let mut partitions: HashMap<u32, SharedPartition> = HashMap::new();

        if let Some(name) = topic_name {
            let storage = Storage::new(path);
            let entries = storage.scan_base();
            for entry in entries{
                let path = entry.expect("could not open entry").path();
                if let Some(partition) = Partition::scan_existing(path, max_segment_bytes){
                    let part_id = partition.id;
                    let shared_partition = Arc::new(Mutex::new(partition));
                    partitions.insert(part_id, shared_partition);
                }
            }
            let partition_count = partitions.len() as u32;
            Some(Topic {
                name,
                partitions,
                storage,
                partition_count,
                next_partition:0
            })
        }else { 
            None
        }
    }

    pub async fn produce(&mut self, msg: Message) -> std::io::Result<(u32, u64)> {
        let partition_id = if let Some(key) = &msg.key{
            self.hash_key_to_partition(key)
        }else {
            let partition_id = self.next_partition;
            self.next_partition = (self.next_partition + 1) % self.partition_count;
            partition_id
        };
        
        let partition = self.partitions.get_mut(&partition_id).expect("Malformed partition map");
        let offset = partition.lock().await.append(&msg)?;
        Ok((partition_id, offset))
    }

    pub fn hash_key_to_partition(&self, key: &[u8]) -> u32 {
        let hash = xxh3_64(key);
        (hash as u32) % self.partition_count
    }
    fn get_dir_name(name: &String) -> String {
        format!("topic_{}", name)
    }
}
