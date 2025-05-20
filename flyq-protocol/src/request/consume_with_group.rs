use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::ProtocolError;

pub struct ConsumeWithGroupRequest {
    pub topic: String,
    pub partition: u32,  // Todo: support for making it option 
    pub group: String,
}

/*
frame: [u32 topic_len][topic bytes][u32 partition][u32 group_len][group bytes]
*/
impl ConsumeWithGroupRequest {
    
    
    pub fn serialize(&self)->Bytes{
        let mut buf = BytesMut::new();
        buf.put_u32(self.topic.len() as u32);
        buf.extend_from_slice(self.topic.as_bytes());
        
        buf.put_u32(self.partition);
        buf.put_u32(self.group.len() as u32);
        buf.extend_from_slice(self.group.as_bytes());
        
        buf.freeze()
    }
    
    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError>{
        
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError("Insufficient data for topic length".into()));
        }
        
        let topic_len = buf.get_u32();

        if buf.remaining() < (topic_len + 4) as usize {
            return Err(ProtocolError::PayloadError("Insufficient data for topic length + partition + group_len".into()));
        }
        let topic = String::from_utf8(buf.split_to(topic_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;
        
        let partition = buf.get_u32();

        let group_len = buf.get_u32();

        if buf.remaining() < group_len as usize {
            return Err(ProtocolError::PayloadError("Buffer too short for group".into()));
        }
        
        let group = String::from_utf8(buf.split_to(group_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in group".into()))?;
        
        Ok(ConsumeWithGroupRequest{
            topic,
            partition,
            group,
        })
    }
    
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn test_serialize_consume_with_group() {
        let topic = "my-topic";
        let group = "analytics-group";

        let req = ConsumeWithGroupRequest {
            topic: topic.into(),
            partition: 3,
            group: group.into(),
        };

        let bytes = req.serialize();

        let expected_len = 4 + topic.len() + 4 + 4 + group.len(); // topic_len + topic + partition + group_len + group
        assert_eq!(bytes.len(), expected_len);
    }

    #[test]
    fn test_roundtrip_consume_with_group() {
        let req = ConsumeWithGroupRequest {
            topic: "orders".into(),
            partition: 2,
            group: "email-worker".into(),
        };

        let bytes = req.serialize();
        let deserialized = ConsumeWithGroupRequest::deserialize(bytes).unwrap();

        assert_eq!(deserialized.topic, req.topic);
        assert_eq!(deserialized.partition, req.partition);
        assert_eq!(deserialized.group, req.group);
    }
}