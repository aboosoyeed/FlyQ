use std::fs;
use std::path::Path;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use anyhow::{Context, Result};

/// Global broker-wide knobs that every partition inherits.
/// Todo: topic to override it .
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    /// Upper bound for a single segment’s size before we rotate.
    pub segment_max_bytes: u64,

    /// Keep data newer than this age. Time wins over size.
    pub retention: Duration,

    /// Soft cap on total on-disk bytes per partition. `None` = disabled.
    pub retention_bytes: Option<u64>,

    /// How often the background cleaner wakes up.
    pub cleanup_interval: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 1024 * 1024 * 1024,          // 1 GiB
            retention: Duration::from_secs(7 * 24 * 60 * 60),   // 7 days
            retention_bytes: None,                              // size-based retention off
            cleanup_interval: Duration::from_secs(60),          // 1 minute
        }
    }
    
}

impl BrokerConfig {

    pub fn load_or_default<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        match path {
            Some(p) => Self::read_from_file(p), // propagate errors unchanged
            None => Ok(Self::default()),
        }
    }
    fn read_from_file<P: AsRef<Path>>(path: P)->Result<Self>{
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading {:?}", path.as_ref()))?;
        let cfg: BrokerConfig = toml::from_str(&raw)
            .with_context(|| "parsing broker config TOML")?;
        Ok(cfg)
    }
}
