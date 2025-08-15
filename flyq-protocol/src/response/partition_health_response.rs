use crate::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub struct PartitionHealthResponse {
    pub topic: String,
    pub partition: u32,
    pub segment_count: u32,
    pub total_size_bytes: u64,
    pub low_watermark: u64,
    pub high_watermark: u64,
    pub log_end_offset: u64,
    pub last_cleanup: Option<u64>, // Unix timestamp in nanoseconds
}

impl PartitionHealthResponse {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Topic
        buf.put_u32(self.topic.len() as u32);
        buf.extend_from_slice(self.topic.as_bytes());
        
        // Partition ID
        buf.put_u32(self.partition);
        
        // Segment metrics
        buf.put_u32(self.segment_count);
        buf.put_u64(self.total_size_bytes);
        
        // Watermarks
        buf.put_u64(self.low_watermark);
        buf.put_u64(self.high_watermark);
        buf.put_u64(self.log_end_offset);
        
        // Last cleanup (optional)
        match self.last_cleanup {
            Some(timestamp) => {
                buf.put_u8(1); // Has timestamp
                buf.put_u64(timestamp);
            }
            None => {
                buf.put_u8(0); // No timestamp
            }
        }
        
        buf.freeze()
    }
    
    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for topic length".into(),
            ));
        }
        let topic_len = buf.get_u32();
        
        if buf.remaining() < topic_len as usize {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for topic".into(),
            ));
        }
        let topic = String::from_utf8(buf.split_to(topic_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;
        
        if buf.remaining() < 37 {  // 4 + 4 + 8 + 8 + 8 + 8 + 1
            return Err(ProtocolError::PayloadError(
                "Insufficient data for partition health metrics".into(),
            ));
        }
        
        let partition = buf.get_u32();
        let segment_count = buf.get_u32();
        let total_size_bytes = buf.get_u64();
        let low_watermark = buf.get_u64();
        let high_watermark = buf.get_u64();
        let log_end_offset = buf.get_u64();
        
        let has_cleanup = buf.get_u8();
        let last_cleanup = if has_cleanup == 1 {
            if buf.remaining() < 8 {
                return Err(ProtocolError::PayloadError(
                    "Insufficient data for last cleanup timestamp".into(),
                ));
            }
            Some(buf.get_u64())
        } else {
            None
        };
        
        Ok(Self {
            topic,
            partition,
            segment_count,
            total_size_bytes,
            low_watermark,
            high_watermark,
            log_end_offset,
            last_cleanup,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_partition_health_roundtrip() {
        let original = PartitionHealthResponse {
            topic: "events".to_string(),
            partition: 0,
            segment_count: 3,
            total_size_bytes: 1024 * 1024 * 100, // 100 MB
            low_watermark: 0,
            high_watermark: 1000,
            log_end_offset: 1000,
            last_cleanup: Some(1234567890),
        };
        
        let bytes = original.serialize();
        let parsed = PartitionHealthResponse::deserialize(bytes).unwrap();
        
        assert_eq!(original.topic, parsed.topic);
        assert_eq!(original.partition, parsed.partition);
        assert_eq!(original.segment_count, parsed.segment_count);
        assert_eq!(original.total_size_bytes, parsed.total_size_bytes);
        assert_eq!(original.low_watermark, parsed.low_watermark);
        assert_eq!(original.high_watermark, parsed.high_watermark);
        assert_eq!(original.log_end_offset, parsed.log_end_offset);
        assert_eq!(original.last_cleanup, parsed.last_cleanup);
    }
    
    #[test]
    fn test_partition_health_no_cleanup() {
        let original = PartitionHealthResponse {
            topic: "logs".to_string(),
            partition: 1,
            segment_count: 1,
            total_size_bytes: 1024,
            low_watermark: 0,
            high_watermark: 10,
            log_end_offset: 10,
            last_cleanup: None,
        };
        
        let bytes = original.serialize();
        let parsed = PartitionHealthResponse::deserialize(bytes).unwrap();
        
        assert_eq!(original.last_cleanup, parsed.last_cleanup);
    }
}