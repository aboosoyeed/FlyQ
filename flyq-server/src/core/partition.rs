use crate::core::partition_state::PartitionState;
use crate::core::partiton_meta::PartitionMeta;
use crate::core::segment::{Segment, SegmentIterator};
use crate::core::storage::Storage;
use crate::core::stored_record::StoredRecord;
use flyq_protocol::errors::DeserializeError;
use flyq_protocol::message::Message;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io;
use tracing::debug;

pub struct Partition {
    pub id: u32,
    pub storage: Storage,                 // ← base directory for segments
    
    // base_offset → segment. consider parking_lot Rwlock if bottleneck comes up with thread starvation 
    pub segments: BTreeMap<u64, Arc<Mutex<Segment>>>,
    
    pub active_segment: u64,
    pub max_segment_bytes: u64,
    pub state: PartitionState,

    pub meta_flush_pending: AtomicBool,
}

impl Partition {
    fn new_segment(&mut self, base_offset: u64) -> std::io::Result<()> {
        let segment = Segment::new(base_offset, &self.storage);
        self.segments
            .insert(base_offset, Arc::new(Mutex::new(segment)));
        self.active_segment = base_offset;

        Ok(())
    }

    pub fn scan_existing(path: PathBuf, max_segment_bytes: u64) -> Option<Partition> {
        let partition_segment = path
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|name| name.strip_prefix("partition_"))
            .map(|s| s.to_string());

        partition_segment.map(|partition_id| {
            Partition::open(
                path,
                partition_id.to_string().parse().unwrap(),
                max_segment_bytes,
            )
            .expect("Could not load partition")
        })
    }

    fn scan_segments(&mut self) -> std::io::Result<()> {
        let entries = self.storage.scan_base();

        for entry in entries {
            let path = entry?.path();
            if let Some(filename) = Segment::scan_path(&path) {
                if let Some((base_offset, next_offset, segment)) =
                    Segment::recover_from_disk(path, &filename)
                {
                    self
                        .segments
                        .insert(base_offset, Arc::new(Mutex::new(segment)));
                    let current_log_end = self.state.log_end_offset();
                    if next_offset > current_log_end {
                        self.state.set_log_end_offset(next_offset);
                        self.active_segment = base_offset;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn open(dir: PathBuf, id: u32, max_segment_bytes: u64) -> std::io::Result<Self> {
        let storage = Storage::new(dir);

        let mut partition = Partition {
            id,
            storage,
            segments: BTreeMap::new(),
            active_segment: 0, // will update below
            max_segment_bytes,
            state: PartitionState::new(0),
            meta_flush_pending: AtomicBool::new(false),
        };

        partition.load_meta()?;
        partition.scan_segments()?;

        if partition.segments.is_empty() {
            partition.new_segment(0)?;
        }

        Ok(partition)
    }

    pub fn append(&mut self, msg: &Message) -> std::io::Result<u64> {
        let offset = self.state.fetch_and_increment_log_end();
        let record = StoredRecord {
            offset,
            message: msg.clone(),
        };
        let bytes = record.serialize();

        // Get active segment (may be replaced if rotated)
        let mut rotate = false;
        if let Some(segment) = self.segments.get(&self.active_segment) {
            let segment = segment.lock().expect("mutex poisoned");
            if segment.size > 0 && segment.size + bytes.len() as u64 > self.max_segment_bytes {
                rotate = true;
            }
        }

        if rotate {
            // Create a new segment starting at current offset
            self.new_segment(offset)?;
        }

        let segment = self
            .segments
            .get(&self.active_segment)
            .expect("active_segment not initialized")
            .clone();
        let mut segment = segment.lock().expect("mutex poisoned");

        self.state.set_high_watermark(offset); // ← for now, fully committed instantly
        self.meta_flush_pending.store(true, Ordering::Relaxed);
        segment.append(offset, &bytes)?;

        debug!(offset, segment = self.active_segment, "Appended message");
        Ok(offset)
    }

    pub fn stream_from_offset(
        &mut self,
        offset: u64,
    ) -> Result<PartitionIterator, DeserializeError> {
        let start_key = self
            .segments
            .iter()
            .rev()
            .find(|(_, seg)| {
                let seg = seg.lock().expect("mutex poisoned");
                seg.base_offset <= offset && seg.last_offset >= offset
            })
            .map(|(&k, _)| k)
            .ok_or( DeserializeError::OffsetNotFound(offset) )?;
        let segments = self.segments.range(start_key..);

        Ok(PartitionIterator {
            segments,
            current_iter: None,
            next_offset: offset,
        })
    }
    pub fn read_from_offset(&mut self, offset: u64) -> Result<Vec<Message>, DeserializeError> {
        self.stream_from_offset(offset)?
            .map(|res| res.map(|(_, msg)| msg)) // discard the offset
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn get_watermark(&self) -> (u64, u64, u64) {
        (
            self.state.low_watermark(),
            self.state.high_watermark(),
            self.state.log_end_offset(),
        )
    }

    pub fn meta_path(&self) -> PathBuf {
        self.storage.base_dir.join("meta.json")
    }

    pub fn persist_meta(&self) -> io::Result<()> {
        let meta = PartitionMeta {
            low_watermark: self.state.low_watermark(),
            high_watermark: self.state.high_watermark(),
            log_end_offset: self.state.log_end_offset(),
        };
        PartitionMeta::save(&self.meta_path(), &meta)?;
        Ok(())
    }

    pub fn load_meta(&mut self) -> io::Result<()> {
        //let meta = PartitionMeta::load(&self.meta_path())?;
        match PartitionMeta::load(&self.meta_path())? {
            Some(meta) => {
                self.state = PartitionState::from_meta(&meta);
            }
            None => {
                self.state = PartitionState::new(0);
            }
        }
        Ok(())
    }
}

pub struct PartitionIterator<'a> {
    segments: Range<'a, u64, Arc<Mutex<Segment>>>, // iterates over Segment Arc wrappers
    current_iter: Option<SegmentIterator>,
    next_offset: u64,
}

impl Iterator for PartitionIterator<'_> {
    type Item = Result<(u64, Message), DeserializeError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have an active iterator, try to get next message
            if let Some(iter) = &mut self.current_iter {
                match iter.next() {
                    Some(Ok((offset, msg))) => {
                        self.next_offset = offset + 1;
                        return Some(Ok((offset, msg)));
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        // current_iter is exhausted, move to next segment
                        self.current_iter = None;
                    }
                }
            }

            // Move to the next segment
            let (_, segment_arc) = self.segments.next()?;
            let (iter_res, last_offset) = {
                let segment = segment_arc.lock().expect("mutex poisoned");
                (segment.stream_from_offset(self.next_offset), segment.last_offset)
            };
            match iter_res {
                Ok(iter) => {
                    self.current_iter = Some(iter);
                    self.next_offset = last_offset + 1;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::partition::Partition;
    use flyq_protocol::message::Message;

    /// Test: Basic append + read on single-partition log
    ///
    /// This tests appends a single message to a new partition and immediately reads it back
    /// using the same offset. It ensures that the append path stores the message correctly
    /// and that the offset/index-based read retrieves the exact message.
    ///
    /// ✅ Verifies:
    ///    - End-to-end flow of `Partition::append` + `read_from_offset`
    ///    - Indexing and file I/O for single-message write
    ///
    #[test]
    fn test_partition_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let partition_dir = dir.path().to_path_buf(); // ← This is now a directory, not a file path

        let mut partition = Partition::open(partition_dir, 0, 1024).unwrap(); // Added max_segment_bytes

        let msg = Message {
            key: Some(b"k".to_vec()),
            value: b"v".to_vec(),
            timestamp: 1,
            headers: None,
        };

        let offset = partition.append(&msg).unwrap();
        let messages = partition.read_from_offset(offset).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, msg.value);
    }

    /// Test: Segment rotation triggers when max_segment_bytes is exceeded
    ///
    /// This tests writes multiple messages to a partition configured with a very small
    /// segment size (e.g., 50 bytes). It confirms that when the active segment grows too large,
    /// the partition automatically rotates to a new segment.
    ///
    /// ✅ Verifies:
    ///    - Segment rotation logic works as expected
    ///    - Messages remain readable across multiple rotated segments
    ///    - Indexing still maintains correct message retrieval
    ///    - All original messages can be read back via `read_from_offset`
    ///    - Segment count > 1 proves rotation occurred
    ///
    #[test]
    fn test_segment_rotation_on_append() {
        let dir = tempfile::tempdir().unwrap();
        let partition_dir = dir.path().to_path_buf();

        // Very small segment size to force rotation (e.g. 50 bytes)
        let mut partition = Partition::open(partition_dir.clone(), 0, 50).unwrap();

        let msg_count = 10;
        let mut offsets = Vec::new();

        for i in 0..msg_count {
            let msg = Message {
                key: Some(format!("key-{}", i).into_bytes()),
                value: format!("value-{}", i).into_bytes(),
                timestamp: 1000 + i,
                headers: None,
            };
            let offset = partition.append(&msg).unwrap();
            offsets.push(offset);
        }

        // Read all messages from the first offset
        let messages = partition.read_from_offset(offsets[0]).unwrap();
        assert_eq!(messages.len() as u64, msg_count);

        for (i, msg) in messages.iter().enumerate() {
            assert_eq!(msg.value, format!("value-{}", i).as_bytes());
            assert_eq!(msg.key.as_ref().unwrap(), format!("key-{}", i).as_bytes());
        }

        // Check segment count
        assert!(
            partition.segments.len() > 1,
            "Expected segment rotation to occur"
        );
    }
}
