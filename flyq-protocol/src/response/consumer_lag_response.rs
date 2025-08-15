use crate::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub struct ConsumerLagResponse {
    pub consumer_group: String,
    pub total_lag: u64,
    pub partitions: Vec<PartitionLag>,
}

#[derive(Debug, Clone)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: u32,
    pub committed_offset: u64,
    pub high_watermark: u64,
    pub lag: u64,
}

impl ConsumerLagResponse {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Consumer group
        buf.put_u32(self.consumer_group.len() as u32);
        buf.extend_from_slice(self.consumer_group.as_bytes());
        
        // Total lag
        buf.put_u64(self.total_lag);
        
        // Partitions
        buf.put_u32(self.partitions.len() as u32);
        for partition in &self.partitions {
            // Topic
            buf.put_u32(partition.topic.len() as u32);
            buf.extend_from_slice(partition.topic.as_bytes());
            
            // Partition ID
            buf.put_u32(partition.partition);
            
            // Offsets and lag
            buf.put_u64(partition.committed_offset);
            buf.put_u64(partition.high_watermark);
            buf.put_u64(partition.lag);
        }
        
        buf.freeze()
    }
    
    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for consumer group length".into(),
            ));
        }
        let group_len = buf.get_u32();
        
        if buf.remaining() < (group_len as usize + 8 + 4) {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for consumer lag response".into(),
            ));
        }
        
        let consumer_group = String::from_utf8(buf.split_to(group_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in consumer group".into()))?;
        
        let total_lag = buf.get_u64();
        let partition_count = buf.get_u32();
        
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            if buf.remaining() < 4 {
                return Err(ProtocolError::PayloadError(
                    "Insufficient data for topic length".into(),
                ));
            }
            let topic_len = buf.get_u32();
            
            if buf.remaining() < (topic_len as usize + 4 + 24) {
                return Err(ProtocolError::PayloadError(
                    "Insufficient data for partition lag".into(),
                ));
            }
            
            let topic = String::from_utf8(buf.split_to(topic_len as usize).to_vec())
                .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;
            
            let partition = buf.get_u32();
            let committed_offset = buf.get_u64();
            let high_watermark = buf.get_u64();
            let lag = buf.get_u64();
            
            partitions.push(PartitionLag {
                topic,
                partition,
                committed_offset,
                high_watermark,
                lag,
            });
        }
        
        Ok(Self {
            consumer_group,
            total_lag,
            partitions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_consumer_lag_roundtrip() {
        let original = ConsumerLagResponse {
            consumer_group: "test-group".to_string(),
            total_lag: 100,
            partitions: vec![
                PartitionLag {
                    topic: "events".to_string(),
                    partition: 0,
                    committed_offset: 50,
                    high_watermark: 100,
                    lag: 50,
                },
                PartitionLag {
                    topic: "events".to_string(),
                    partition: 1,
                    committed_offset: 75,
                    high_watermark: 125,
                    lag: 50,
                },
            ],
        };
        
        let bytes = original.serialize();
        let parsed = ConsumerLagResponse::deserialize(bytes).unwrap();
        
        assert_eq!(original.consumer_group, parsed.consumer_group);
        assert_eq!(original.total_lag, parsed.total_lag);
        assert_eq!(original.partitions.len(), parsed.partitions.len());
        
        for (orig, parsed) in original.partitions.iter().zip(parsed.partitions.iter()) {
            assert_eq!(orig.topic, parsed.topic);
            assert_eq!(orig.partition, parsed.partition);
            assert_eq!(orig.committed_offset, parsed.committed_offset);
            assert_eq!(orig.high_watermark, parsed.high_watermark);
            assert_eq!(orig.lag, parsed.lag);
        }
    }
}