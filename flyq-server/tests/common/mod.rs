use std::fs;
use std::path::{Path, PathBuf};

fn clear_folder<P: AsRef<Path>>(folder_path: P) -> std::io::Result<()> {
    for entry in fs::read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

pub fn folder_to_use() -> PathBuf {
    tempfile::Builder::new()
        .prefix("flyq_test_")
        .tempdir()
        .expect("failed to create temp dir")
        .into_path()
}
