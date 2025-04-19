use crate::core::message::Message;
use crate::core::segment::{Segment, SegmentIterator};
use crate::core::storage::Storage;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing::debug;
use crate::core::error::DeserializeError;

pub struct Partition {
    pub id: u32,
    pub storage: Storage,                 // ← base directory for segments
    pub segments: BTreeMap<u64, Segment>, // base_offset → segment
    pub active_segment: u64,
    pub next_offset: u64,
    pub max_segment_bytes: u64,
}

impl Partition {
    fn new_segment(&mut self, base_offset: u64) -> std::io::Result<()> {
        let segment = Segment::new(base_offset, &self.storage);
        self.segments.insert(base_offset, segment);
        self.active_segment = base_offset;

        Ok(())
    }

    pub fn scan_existing(path: PathBuf, max_segment_bytes: u64) -> Option<Partition> {
        let partition_segment = path
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|name| name.strip_prefix("partition_"))
            .map(|s| s.to_string());

        partition_segment.map(|partition_id| Partition::open(
                    path,
                    partition_id.to_string().parse().unwrap(),
                    max_segment_bytes,
                )
                .expect("Could not load partition"))
    }

    fn scan_segments(&mut self) -> std::io::Result<()> {
        let entries = self.storage.scan_base();

        for entry in entries {
            let path = entry?.path();
            if let Some(filename) = Segment::scan_path(&path) {
                if let Some((base_offset, next_offset, segment)) =
                    Segment::recover_from_disk(path, &filename)
                {
                    self.segments.insert(base_offset, segment);

                    if next_offset > self.next_offset {
                        self.next_offset = next_offset;
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
            next_offset: 0,
            max_segment_bytes,
        };

        partition.scan_segments()?;

        if partition.segments.is_empty() {
            partition.new_segment(0)?;
        }

        Ok(partition)
    }

    pub fn append(&mut self, msg: &Message) -> std::io::Result<u64> {
        let offset = self.next_offset;
        let bytes = msg.serialize(offset);

        // Get active segment (may be replaced if rotated)
        let mut rotate = false;
        if let Some(segment) = self.segments.get(&self.active_segment) {
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
            .get_mut(&self.active_segment)
            .expect("active_segment not initialized");

        self.next_offset += 1;
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
            .find(|(_, seg)| seg.base_offset <= offset && seg.last_offset >= offset)
            .map(|(&k, _)| k)
            .ok_or_else(|| {
                DeserializeError::InvalidFormat(format!(
                    "Offset {} not found in any segment",
                    offset
                ))
            })?;
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
}

pub struct PartitionIterator<'a> {
    segments: Range<'a, u64, Segment>, // iterates over Segment references
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
            let (_, segment) = self.segments.next()?;
            match segment.stream_from_offset(self.next_offset) {
                Ok(iter) => {
                    self.current_iter = Some(iter);
                    self.next_offset = segment.last_offset + 1;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::message::Message;
    use crate::core::partition::Partition;

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
        assert_eq!(messages.len(), msg_count.try_into().unwrap());

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
