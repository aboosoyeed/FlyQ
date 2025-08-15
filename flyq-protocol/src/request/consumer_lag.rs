use crate::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct ConsumerLagRequest {
    pub consumer_group: String,
    pub topics: Option<Vec<String>>,
}

impl ConsumerLagRequest {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Consumer group
        buf.put_u32(self.consumer_group.len() as u32);
        buf.extend_from_slice(self.consumer_group.as_bytes());
        
        // Topics (optional)
        match &self.topics {
            Some(topics) => {
                buf.put_u8(1); // Has topics
                buf.put_u32(topics.len() as u32);
                for topic in topics {
                    buf.put_u32(topic.len() as u32);
                    buf.extend_from_slice(topic.as_bytes());
                }
            }
            None => {
                buf.put_u8(0); // No topics (all subscribed)
            }
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
        
        if buf.remaining() < group_len as usize {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for consumer group".into(),
            ));
        }
        let consumer_group = String::from_utf8(buf.split_to(group_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in consumer group".into()))?;
        
        if buf.remaining() < 1 {
            return Err(ProtocolError::PayloadError(
                "Missing topics flag".into(),
            ));
        }
        let has_topics = buf.get_u8();
        
        let topics = if has_topics == 1 {
            if buf.remaining() < 4 {
                return Err(ProtocolError::PayloadError(
                    "Insufficient data for topics count".into(),
                ));
            }
            let topic_count = buf.get_u32();
            let mut topics = Vec::with_capacity(topic_count as usize);
            
            for _ in 0..topic_count {
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
                topics.push(topic);
            }
            Some(topics)
        } else {
            None
        };
        
        Ok(Self {
            consumer_group,
            topics,
        })
    }
}