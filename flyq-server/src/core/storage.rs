use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

/// Storage abstraction for managing segment and index files in FlyQ.
/// Provides a centralized interface for all file operations related to
/// segments, partitions, and their associated index files.
pub struct Storage {
    pub base_dir: PathBuf,
}

impl Storage {
    /// Creates a new Storage instance with the specified base directory.
    pub fn new<P: AsRef<Path>>(base_dir: P) -> io::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir)?;
        Ok(Self { base_dir })
    }

    /// Creates or opens a segment file and its associated index file.
    /// Returns (segment_file, index_file, segment_path, index_path)
    pub fn create_segment(&self, base_offset: u64) -> io::Result<(File, File, PathBuf, PathBuf)> {
        let segment_path = self.segment_path(base_offset);
        let index_path = self.index_path(base_offset);

        let segment_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&segment_path)?;

        let index_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&index_path)?;

        Ok((segment_file, index_file, segment_path, index_path))
    }

    /// Opens an existing segment file for reading/appending.
    pub fn open_segment(&self, base_offset: u64) -> io::Result<(File, PathBuf)> {
        let path = self.segment_path(base_offset);
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)?;
        Ok((file, path))
    }

    /// Opens an existing segment file in read-only mode.
    pub fn open_segment_readonly(&self, base_offset: u64) -> io::Result<File> {
        let path = self.segment_path(base_offset);
        File::open(path)
    }

    /// Deletes a segment and its associated index file.
    pub fn delete_segment(&self, base_offset: u64) -> io::Result<()> {
        let segment_path = self.segment_path(base_offset);
        let index_path = self.index_path(base_offset);

        if segment_path.exists() {
            fs::remove_file(&segment_path)?;
            tracing::info!("Deleted segment file: {:?}", segment_path);
        }

        if index_path.exists() {
            fs::remove_file(&index_path)?;
            tracing::info!("Deleted index file: {:?}", index_path);
        }

        Ok(())
    }

    /// Lists all segment files in the storage directory.
    /// Returns a sorted vector of (base_offset, file_size) tuples.
    pub fn list_segments(&self) -> io::Result<Vec<(u64, u64)>> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                if let Some(base_offset) = Self::parse_segment_filename(filename) {
                    let metadata = entry.metadata()?;
                    segments.push((base_offset, metadata.len()));
                }
            }
        }

        segments.sort_by_key(|(offset, _)| *offset);
        Ok(segments)
    }

    /// Checks if a segment exists.
    pub fn segment_exists(&self, base_offset: u64) -> bool {
        self.segment_path(base_offset).exists()
    }

    /// Gets the size of a segment file.
    pub fn get_segment_size(&self, base_offset: u64) -> io::Result<u64> {
        let path = self.segment_path(base_offset);
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }

    /// Loads an index from disk into a BTreeMap.
    /// Returns (index_map, last_offset_in_index)
    pub fn load_index(&self, base_offset: u64) -> io::Result<(BTreeMap<u64, u64>, u64)> {
        let index_path = self.index_path(base_offset);
        
        if !index_path.exists() {
            return Ok((BTreeMap::new(), 0));
        }

        let mut index = BTreeMap::new();
        let mut last_offset = 0;

        let mut file = File::open(&index_path)?;
        let mut reader = BufReader::new(file);
        
        // Index is stored in binary format: 8 bytes offset + 8 bytes position
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
            last_offset = last_offset.max(offset);
        }

        Ok((index, last_offset))
    }

    /// Recovers a segment from disk, reading its metadata and index.
    /// Returns (base_offset, file_size, segment_file, index_file, index_map, last_offset)
    pub fn recover_segment(&self, filename: &str) -> io::Result<Option<(u64, u64, File, File, BTreeMap<u64, u64>, u64)>> {
        if let Some(base_offset) = Self::parse_segment_filename(filename) {
            let segment_path = self.segment_path(base_offset);
            let index_path = self.index_path(base_offset);

            let segment_file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&segment_path)?;

            let size = segment_file.metadata()?.len();

            let index_file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(&index_path)?;

            let (index, last_offset) = self.load_index(base_offset)?;

            Ok(Some((base_offset, size, segment_file, index_file, index, last_offset)))
        } else {
            Ok(None)
        }
    }

    /// Scans for partition directories in the storage.
    /// Returns a vector of partition IDs found.
    pub fn list_partitions(&self) -> io::Result<Vec<u32>> {
        let mut partitions = Vec::new();

        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
                    if let Some(partition_id) = Self::parse_partition_dirname(filename) {
                        partitions.push(partition_id);
                    }
                }
            }
        }

        partitions.sort();
        Ok(partitions)
    }

    /// Creates a partition directory.
    pub fn create_partition_dir(&self, partition_id: u32) -> io::Result<PathBuf> {
        let path = self.partition_path(partition_id);
        fs::create_dir_all(&path)?;
        Ok(path)
    }

    /// Returns the path for a segment file.
    fn segment_path(&self, base_offset: u64) -> PathBuf {
        self.base_dir.join(Self::segment_filename(base_offset))
    }

    /// Returns the path for an index file.
    fn index_path(&self, base_offset: u64) -> PathBuf {
        self.base_dir.join(Self::index_filename(base_offset))
    }

    /// Returns the path for a partition directory.
    fn partition_path(&self, partition_id: u32) -> PathBuf {
        self.base_dir.join(Self::partition_dirname(partition_id))
    }

    /// Generates a segment filename from base offset.
    pub fn segment_filename(base_offset: u64) -> String {
        format!("segment_{:020}.log", base_offset)
    }

    /// Generates an index filename from base offset.
    pub fn index_filename(base_offset: u64) -> String {
        format!("segment_{:020}.index", base_offset)
    }

    /// Generates a partition directory name.
    pub fn partition_dirname(partition_id: u32) -> String {
        format!("partition-{}", partition_id)
    }

    /// Parses a segment filename to extract the base offset.
    pub fn parse_segment_filename(filename: &str) -> Option<u64> {
        filename
            .strip_prefix("segment_")?
            .strip_suffix(".log")
            .and_then(|s| s.parse::<u64>().ok())
    }

    /// Parses a partition directory name to extract the partition ID.
    fn parse_partition_dirname(dirname: &str) -> Option<u32> {
        dirname
            .strip_prefix("partition-")
            .and_then(|s| s.parse::<u32>().ok())
    }

    /// Helper to open any file with standard options for FlyQ.
    pub fn open_file(&self, filename: &str) -> io::Result<File> {
        let path = self.base_dir.join(filename);
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
    }

    /// Gets the base directory path.
    pub fn base_path(&self) -> &Path {
        &self.base_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_segment_operations() {
        let dir = TempDir::new().unwrap();
        let storage = Storage::new(dir.path()).unwrap();

        // Test segment creation
        let (_seg_file, _idx_file, seg_path, idx_path) = storage.create_segment(0).unwrap();
        assert!(seg_path.exists());
        assert!(idx_path.exists());

        // Test segment exists
        assert!(storage.segment_exists(0));
        assert!(!storage.segment_exists(100));

        // Test list segments
        storage.create_segment(100).unwrap();
        storage.create_segment(200).unwrap();
        let segments = storage.list_segments().unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].0, 0);
        assert_eq!(segments[1].0, 100);
        assert_eq!(segments[2].0, 200);

        // Test delete segment
        storage.delete_segment(100).unwrap();
        assert!(!storage.segment_exists(100));
        let segments = storage.list_segments().unwrap();
        assert_eq!(segments.len(), 2);
    }

    #[test]
    fn test_filename_parsing() {
        assert_eq!(Storage::parse_segment_filename("segment_00000000000000000000.log"), Some(0));
        assert_eq!(Storage::parse_segment_filename("segment_00000000000000001000.log"), Some(1000));
        assert_eq!(Storage::parse_segment_filename("invalid.log"), None);
        assert_eq!(Storage::parse_segment_filename("segment_abc.log"), None);
    }

    #[test]
    fn test_partition_operations() {
        let dir = TempDir::new().unwrap();
        let storage = Storage::new(dir.path()).unwrap();

        // Create partitions
        storage.create_partition_dir(0).unwrap();
        storage.create_partition_dir(1).unwrap();
        storage.create_partition_dir(2).unwrap();

        // List partitions
        let partitions = storage.list_partitions().unwrap();
        assert_eq!(partitions, vec![0, 1, 2]);
    }
}