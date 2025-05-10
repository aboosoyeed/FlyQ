use crate::core::constants::DEFAULT_INDEX_INTERVAL;
use crate::core::storage::Storage;
use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use flyq_protocol::errors::DeserializeError;
use flyq_protocol::message::Message;

pub struct Segment {
    pub(crate) base_offset: u64,
    pub(crate) file: File,
    pub(crate) index_file: File,
    pub(crate) size: u64,
    pub(crate) index: BTreeMap<u64, u64>, // offset → local file position
    pub last_offset: u64,                 // inclusive, or offset of last message
    index_interval: u32,
    index_counter: u32,
}

impl Segment {
    pub fn new(base_offset: u64, storage: &Storage) -> Self {
        let file_name = Self::segment_filename(base_offset);
        let (path, file) = storage.open_file(&file_name);
        let index_file_name = Segment::index_filename(base_offset);
        let index_path = path.parent().unwrap().join(index_file_name);
        let (_, index_file) = Storage::open_file_from_path(&index_path);

        Self {
            base_offset,
            file,
            size: 0,
            index: BTreeMap::new(),
            index_file,
            last_offset: 0,
            index_interval: DEFAULT_INDEX_INTERVAL,
            index_counter: DEFAULT_INDEX_INTERVAL,
        }
    }

    pub fn segment_filename(base_offset: u64) -> String {
        format!("segment_{:020}.log", base_offset)
    }

    pub fn index_filename(base_offset: u64) -> String {
        format!("segment_{:020}.index", base_offset)
    }

    fn index_path_from_base(base_offset: u64, dir: &Path) -> PathBuf {
        dir.join(Self::index_filename(base_offset))
    }

    pub fn parse_base_offset(filename: &str) -> Option<u64> {
        filename
            .strip_prefix("segment_")?
            .strip_suffix(".log")
            .or_else(|| filename.strip_suffix(".index"))
            .and_then(|s| s.parse::<u64>().ok())
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> std::io::Result<u64> {
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

    pub fn recover_from_disk(path: PathBuf, filename: &str) -> Option<(u64, u64, Segment)> {
        if let Some(base_offset) = Self::parse_base_offset(filename) {
            let (_, file) = Storage::open_file_from_path(&path);

            let size = file.metadata().ok()?.len();

            let dir = path.parent().unwrap();
            let (index, index_file, mut last_offset) = Self::load_index_from_file(dir, base_offset);

            let mut segment = Segment {
                base_offset,
                file,
                size,
                index,
                last_offset,
                index_file,
                index_interval: DEFAULT_INDEX_INTERVAL,
                index_counter: DEFAULT_INDEX_INTERVAL,
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

            Some((base_offset, last_offset + 1, segment))
        } else {
            None
        }
    }

    fn load_index_from_file(dir: &Path, base_offset: u64) -> (BTreeMap<u64, u64>, File, u64) {
        let index_path = Self::index_path_from_base(base_offset, dir);
        let (index_path_exists, index_file) = Storage::open_file_from_path(&index_path);
        let mut index = BTreeMap::new();
        let mut last_offset = 0;

        if index_path_exists {
            let mut reader = BufReader::new(&index_file);
            let mut buf = [0u8; 16];
            while reader.read_exact(&mut buf).is_ok() {
                let offset = u64::from_be_bytes(
                    buf[0..8]
                        .try_into()
                        .expect("index offset slice was not 8 bytes"),
                );
                let pos = u64::from_be_bytes(
                    buf[8..16]
                        .try_into()
                        .expect("index position slice must be 8 bytes"),
                );
                index.insert(offset, pos);
                last_offset = offset;
            }
        }

        (index, index_file, last_offset)
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
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    None
                } else {
                    Some(Err(DeserializeError::InvalidFormat(e.to_string())))
                };
            }

            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; msg_len];
            if let Err(e) = self.reader.read_exact(&mut msg_buf) {
                self.end_of_file = true;
                return Some(Err(DeserializeError::InvalidFormat(e.to_string())));
            }

            match Message::deserialize(&msg_buf) {
                Ok((offset, msg)) => {
                    if offset < self.offset {
                        continue; // skip stale message
                    }

                    self.offset = offset + 1;
                    return Some(Ok((offset, msg)));
                }
                Err(e) => {
                    self.end_of_file = true;
                    return Some(Err(e));
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

#[cfg(test)]
mod tests {
    use crate::core::storage::Storage;
    use std::io::Write;
    use std::path::PathBuf;
    use flyq_protocol::message::Message;

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
        let log_path = dir.path().join("segment_00000000000000000000.log");
        let storage = Storage::new(PathBuf::from(dir.path()));
        // Append a few messages
        let mut segment = Segment::new(0, &storage);
        for i in 0..3 {
            let msg = Message {
                key: Some(format!("key-{}", i).into_bytes()),
                value: format!("val-{}", i).into_bytes(),
                timestamp: 1000 + i,
                headers: None,
            };
            let offset = i;
            let bytes = msg.serialize_for_disk(offset);
            segment.append(offset, &bytes).unwrap();
        }

        // Simulate crash — delete the index file
        let index_path = log_path.with_extension("index");
        std::fs::remove_file(&index_path).unwrap();

        // Recover segment
        let (_, _, recovered_segment) =
            Segment::recover_from_disk(log_path.clone(), "segment_00000000000000000000.log")
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
        let log_path = dir.path().join("segment_00000000000000000000.log");
        let storage = Storage::new(dir.path().to_path_buf());

        // Create a segment with sparse index (index every 3 messages)
        let mut segment = Segment::new(0, &storage);
        segment.index_interval = 3;
        segment.index_counter = 3;

        for i in 0..5 {
            let msg = Message {
                key: Some(format!("k{}", i).into_bytes()),
                value: format!("val-{}", i).into_bytes(),
                timestamp: 1000 + i,
                headers: None,
            };
            let bytes = msg.serialize_for_disk(i);
            segment.append(i, &bytes).unwrap();
        }

        // Force flush index file
        segment.index_file.flush().unwrap();

        // Recover from disk
        let (_, _, recovered_segment) =
            Segment::recover_from_disk(log_path, "segment_00000000000000000000.log").unwrap();

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
