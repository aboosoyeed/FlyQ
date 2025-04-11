use std::fs;
use std::fs::{create_dir_all, File, OpenOptions, ReadDir};
use std::path::PathBuf;

pub struct Storage{
    base_dir: PathBuf
}

impl Storage{
    pub fn new(base_dir:PathBuf)->Self{
        
        // This is where all segment_*.log files will live
        create_dir_all(&base_dir).expect("could not create base directory"); // make sure it exists
        
        Self{
            base_dir
        }
    }
    
    pub fn open_file(&self, file_name:&str) ->(PathBuf,File){
        let path = self.base_dir.join(file_name);
        let (_,file) = Self::open_file_from_path(&path);
        (path,file)
    }
    
    pub fn scan_base(&self) -> ReadDir {
        fs::read_dir(&self.base_dir).expect("could not scan base directory") // this is safe as open has already tested it
    }
    
    pub fn open_file_from_path(path: &PathBuf) ->(bool, File){
        let exists = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .append(true) // ✅ must be writable
            .create(true)
            .open(path).unwrap_or_else(|_|panic!("could not open path {:?}", path));
        (exists, file)
    }

    
    
}

