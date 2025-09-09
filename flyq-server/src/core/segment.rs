use crate::core::constants::DEFAULT_INDEX_INTERVAL;
use crate::core::storage::Storage;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use flyq_protocol::errors::DeserializeError;
use flyq_protocol::message::Message;
use crate::core::stored_record::StoredRecord;

pub struct Segment {
    pub(crate) base_offset: u64,
    pub(crate) segment_path: PathBuf,
    pub(crate) index_path: PathBuf,
    pub(crate) file: File,
    pub(crate) index_file: File,
    pub(crate) size: u64,
    pub(crate) index: BTreeMap<u64, u64>, // offset → local file position
    pub last_offset: u64,                 // inclusive, or offset of last message
    index_interval: u32,
    index_counter: u32,
    pub last_write_ns: AtomicU64,
    pub mark_deleted: AtomicBool

}

impl Segment {
    pub fn new(base_offset: u64, storage: &Storage) -> std::io::Result<Self> {
        let (file, index_file, segment_path, index_path) = storage.create_segment(base_offset)?;

        Ok(Self {
            base_offset,
            segment_path,
            index_path,
            file,
            size: 0,
            index: BTreeMap::new(),
            index_file,
            last_offset: 0,
            index_interval: DEFAULT_INDEX_INTERVAL,
            index_counter: DEFAULT_INDEX_INTERVAL,
            last_write_ns: AtomicU64::new(now_ns()),
            mark_deleted: AtomicBool::new(false),
        })
    }

    pub fn segment_filename(base_offset: u64) -> String {
        Storage::segment_filename(base_offset)
    }

    pub fn index_filename(base_offset: u64) -> String {
        Storage::index_filename(base_offset)
    }


    pub fn parse_base_offset(filename: &str) -> Option<u64> {
        Storage::parse_segment_filename(filename)
            .or_else(|| filename
                .strip_prefix("segment_")?
                .strip_suffix(".index")
                .and_then(|s| s.parse::<u64>().ok()))
    }

    pub fn last_write(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(self.last_write_ns.load(Ordering::Acquire))
    }

    pub fn mark_deleted(&self )-> std::io::Result<()>{
        self.mark_deleted.store(true, Ordering::Release);
        Ok(())
    }

    fn delete_files(&self) -> std::io::Result<()> {
        // Only delete if marked for deletion
        if !self.mark_deleted.load(Ordering::Acquire) {
            return Ok(());
        }

        // Delete segment file
        if self.segment_path.exists() {
            fs::remove_file(&self.segment_path)?;
            tracing::info!("Deleted segment file: {:?}", self.segment_path);
        }

        // Delete index file  
        if self.index_path.exists() {
            fs::remove_file(&self.index_path)?;
            tracing::info!("Deleted index file: {:?}", self.index_path);
        }

        Ok(())
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> std::io::Result<u64> {
        // Update last write timestamp
        self.last_write_ns.store(now_ns(), Ordering::Release);
        
        // Seek to end first (optional, but clean)
        self.file.seek(SeekFrom::End(0))?;

        let pos = self.file.stream_position()?; // ✅ NOW CORRECT
        self.file.write_all(bytes)?;
        self.file.flush()?; // or leave it for batch control

        self.size += bytes.len() as u64;
        self.last_offset = self.last_offset.max(offset); // protects against incorrect overwrites
        if self.should_index(offset) {
            self.create_index(offset, pos);
        }
        Ok(offset)
    }

    fn create_index(&mut self, offset: u64, pos: u64) {
        self.index.insert(offset, pos);
        self.write_index_entry(offset, pos);
        self.index_counter = self.index_interval;
    }

    fn write_index_entry(&mut self, offset: u64, pos: u64) {
        let mut entry = [0u8; 16];
        entry[0..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..16].copy_from_slice(&pos.to_be_bytes());

        self.index_file
            .write_all(&entry)
            .expect("index write failed");
        self.index_file.flush().expect("index flush failed");
    }

    fn should_index(&mut self, offset: u64) -> bool {
        if offset == self.base_offset {
            return true; // Always index first message in segment
        }

        if self.index_counter == 0 {
            self.index_counter = self.index_interval;
            return true;
        }

        self.index_counter -= 1;
        false
    }

    pub fn stream_from_offset(&self, offset: u64) -> Result<SegmentIterator, DeserializeError> {
        let closest_pos = if self.index.is_empty() {
            0 // fallback: start of file
        } else {
            self.index
                .range(..=offset)
                .next_back()
                .map(|(_, &v)| v)
                .unwrap_or(0) // fallback if no index entry ≤ offset
        };

        let mut file = self
            .file
            .try_clone()
            .map_err(|e| DeserializeError::InvalidFormat(e.to_string()))?;
        file.seek(SeekFrom::Start(closest_pos))
            .map_err(|e| DeserializeError::InvalidFormat(e.to_string()))?;

        Ok(SegmentIterator {
            reader: BufReader::new(file),
            offset,
            end_of_file: false,
        })
    }

    pub fn scan_path(path: &Path) -> Option<String> {
        path.file_name()
            .and_then(|f| f.to_str())
            .map(|s| s.to_string())
            .filter(|filename| filename.starts_with("segment_") && filename.ends_with(".log"))
    }

    pub fn recover_from_disk(storage: &Storage, filename: &str) -> std::io::Result<Option<(u64, u64, Segment)>> {
        if let Some((base_offset, size, file, index_file, index, mut last_offset)) = storage.recover_segment(filename)? {
            let segment_path = storage.base_path().join(Storage::segment_filename(base_offset));
            let index_path = storage.base_path().join(Storage::index_filename(base_offset));
            
            let mut segment = Segment {
                base_offset,
                segment_path,
                index_path,
                file,
                size,
                index,
                last_offset,
                index_file,
                index_interval: DEFAULT_INDEX_INTERVAL,
                index_counter: DEFAULT_INDEX_INTERVAL,
                last_write_ns: AtomicU64::new(now_ns()),
                mark_deleted: AtomicBool::new(false),
            };

            // Try recovering from beyond the last known offset
            let resume_offset = last_offset + 1;

            if let Ok(iter) = segment.stream_from_offset(resume_offset) {
                for msg in iter {
                    match msg {
                        Ok((offset, _msg)) => {
                            segment.last_offset = segment.last_offset.max(offset);
                            last_offset = segment.last_offset;
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to parse message in segment {}: {:?}",
                                segment.base_offset, e
                            );
                            break; // stop recovery on parse failure
                        }
                    }
                }
            }

            Ok(Some((base_offset, last_offset + 1, segment)))
        } else {
            Ok(None)
        }
    }

}

pub struct SegmentIterator {
    reader: BufReader<File>,
    offset: u64,
    end_of_file: bool,
}

impl Iterator for SegmentIterator {
    type Item = Result<(u64, Message), DeserializeError>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.end_of_file {
            let mut len_buf = [0u8; 4];
            if let Err(e) = self.reader.read_exact(&mut len_buf) {
                self.end_of_file = true;
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => None,
                    _ => Some(Err(DeserializeError::InvalidFormat(e.to_string()))),
                };
            }

            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; msg_len];
            if let Err(e) = self.reader.read_exact(&mut msg_buf) {
                self.end_of_file = true;
                return Some(Err(DeserializeError::InvalidFormat(e.to_string())));
            }
            return match StoredRecord::deserialize(&msg_buf) {
                Ok(record) => {
                    if record.offset < self.offset {
                        continue; // skip stale message
                    }

                    self.offset = record.offset + 1;
                    Some(Ok((record.offset, record.message)))
                }
                Err(e) => {
                    self.end_of_file = true;
                    Some(Err(e))
                }
            }

        }

        None
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Segment(base_offset={}, last_offset={}, size={}, index_size={})",
            self.base_offset,
            self.last_offset,
            self.size,
            self.index.len()
        )
    }
}

#[inline(always)]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Only delete files if marked for deletion
        if self.mark_deleted.load(Ordering::Acquire) {
            if let Err(e) = self.delete_files() {
                tracing::warn!(
                    error = ?e,
                    segment = ?self.segment_path,
                    "Failed to delete segment files during drop"
                );
            } else {
                tracing::debug!("Segment files deleted on drop: {:?}", self.segment_path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::storage::Storage;
    use std::io::Write;
    use flyq_protocol::message::Message;
    use crate::core::stored_record::StoredRecord;

    /// Test: Segment recovery when index file is missing (e.g., crash scenario)
    ///
    /// This tests simulates a segment that has written some messages but then "crashed"
    /// before persisting the index file. On recovery, the segment should still be
    /// able to stream and read messages correctly from disk, even without any index
    /// entries.
    ///
    /// ✅ Verifies fallback logic in `stream_from_offset()`:
    ///    - When index is missing or empty, reading should still work by scanning from the file start.
    ///
    #[test]
    fn test_segment_read_without_index_file() {
        use crate::core::segment::Segment;

        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path()).unwrap();
        // Append a few messages
        let mut segment = Segment::new(0, &storage).unwrap();
        for i in 0..3 {
            let msg = Message {
                key: Some(format!("key-{}", i).into_bytes()),
                value: format!("val-{}", i).into_bytes(),
                timestamp: 1000 + i,
                headers: None,
            };
            let record = StoredRecord { offset: i, message: msg };
            let bytes = record.serialize();
            segment.append(i, &bytes).unwrap();
        }

        // Simulate crash — delete the index file
        let index_path = dir.path().join("segment_00000000000000000000.index");
        std::fs::remove_file(&index_path).unwrap();

        // Recover segment
        let (_, _, recovered_segment) =
            Segment::recover_from_disk(&storage, "segment_00000000000000000000.log")
                .unwrap()
                .unwrap();

        // Can we stream from offset 1 even without an index?
        let mut iter = recovered_segment.stream_from_offset(1).unwrap();
        let (_, first) = iter.next().unwrap().unwrap();
        assert_eq!(first.value, b"val-1");
    }

    /// Test: Segment with sparse index can recover and read all messages
    ///
    /// This tests writes messages to a segment with a sparse indexing interval (e.g., every 3rd message),
    /// then recovers from disk and attempts to read all messages back. It verifies that
    /// even offsets that are not directly indexed are still discoverable and readable,
    /// thanks to scanning from the nearest prior index entry.
    ///
    /// ✅ Verifies:
    ///    - Sparse indexing logic works as intended
    ///    - Recovery loads only partial index
    ///    - `stream_from_offset()` can traverse gaps in the index
    ///    - Messages are deserialized in correct order
    ///
    #[test]
    fn test_segment_sparse_index_read_all() {
        use crate::core::segment::Segment;
        use crate::core::storage::Storage;

        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path()).unwrap();

        // Create a segment with sparse index (index every 3 messages)
        let mut segment = Segment::new(0, &storage).unwrap();
        segment.index_interval = 3;
        segment.index_counter = 3;

        for i in 0..5 {
            let msg = Message {
                key: Some(format!("k{}", i).into_bytes()),
                value: format!("val-{}", i).into_bytes(),
                timestamp: 1000 + i,
                headers: None,
            };
            let record = StoredRecord { offset: i, message: msg };
            let bytes = record.serialize();
            segment.append(i, &bytes).unwrap();
        }

        // Force flush index file
        segment.index_file.flush().unwrap();

        // Recover from disk
        let (_, _, recovered_segment) =
            Segment::recover_from_disk(&storage, "segment_00000000000000000000.log").unwrap().unwrap();

        // Try to stream from offset 0 (even if not all are indexed)
        let stream = recovered_segment.stream_from_offset(0).expect("stream ok");
        let messages: Vec<_> = stream.map(|r| r.unwrap().1.value).collect();

        let expected = vec![
            b"val-0".to_vec(),
            b"val-1".to_vec(),
            b"val-2".to_vec(),
            b"val-3".to_vec(),
            b"val-4".to_vec(),
        ];

        assert_eq!(messages, expected);
    }
}
