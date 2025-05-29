use std::sync::atomic::{AtomicU64, Ordering};
use crate::core::partiton_meta::PartitionMeta;

pub struct PartitionState {
    log_end_offset: AtomicU64,
    low_watermark: AtomicU64,
    high_watermark: AtomicU64,
}

impl PartitionState {
    pub fn new(start_offset: u64) -> Self {
        Self {
            log_end_offset: AtomicU64::new(start_offset),
            low_watermark: AtomicU64::new(start_offset),
            high_watermark: AtomicU64::new(start_offset),
        }
    }

    pub fn from_meta(meta: &PartitionMeta) -> Self {
        Self {
            log_end_offset: AtomicU64::new(meta.log_end_offset),
            low_watermark: AtomicU64::new(meta.low_watermark),
            high_watermark: AtomicU64::new(meta.high_watermark),
        }
    }
    
    pub fn fetch_and_increment_log_end(&self) -> u64 {
        self.log_end_offset.fetch_add(1, Ordering::SeqCst)
    }

    pub fn log_end_offset(&self) -> u64 {
        self.log_end_offset.load(Ordering::SeqCst)
    }

    pub fn set_log_end_offset(&self, val: u64) {
        self.log_end_offset.store(val, Ordering::SeqCst)
    }

    pub fn low_watermark(&self) -> u64 {
        self.low_watermark.load(Ordering::SeqCst)
    }
    
    // this is always set to 0 for now
    // will be used when we intro log truncation
    pub fn set_low_watermark(&self, val: u64) {
        self.low_watermark.store(val, Ordering::SeqCst)
    }

    pub fn high_watermark(&self) -> u64 {
        self.high_watermark.load(Ordering::SeqCst)
    }
    
    // for now this is set at append .. 
    // will modify this when we ack replica 
    pub fn set_high_watermark(&self, val: u64) {
        self.high_watermark.store(val, Ordering::SeqCst)
    }
}