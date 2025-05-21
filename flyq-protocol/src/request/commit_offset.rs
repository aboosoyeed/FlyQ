use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::ProtocolError;

pub struct CommitOffsetRequest {
    pub topic: String,
    pub partition: u32,
    pub group: String,
    pub offset: u64,
}

//frame: [u32 topic_len][topic bytes][u32 partition][u32 group_len][group bytes][u64 offset]

impl CommitOffsetRequest {
    
    pub fn serialize(&self) -> Bytes{
        let mut buf = BytesMut::new();
        buf.put_u32(self.topic.len() as u32);
        buf.extend_from_slice(self.topic.as_bytes());
        buf.put_u32(self.partition);
        buf.put_u32(self.group.len() as u32);
        buf.extend_from_slice(self.group.as_bytes());
        buf.put_u64(self.offset);
        buf.freeze()
    }
    
    pub fn deserialize(mut buf:Bytes) -> Result<Self, ProtocolError>{
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError("Insufficient data for topic length".into()));
        }
        let topic_len = buf.get_u32();
        if buf.remaining() < (topic_len + 4 + 4) as usize {
            return Err(ProtocolError::PayloadError("Insufficient data for topic + partition + group length".into()));
        }
        let topic = String::from_utf8(buf.split_to(topic_len as usize).to_vec())
            .map_err(|_|ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;
        let partition = buf.get_u32();
        let group_len = buf.get_u32();
        
        if buf.remaining() < (group_len + 4) as usize {
            return Err(ProtocolError::PayloadError("Insufficient data for group length + offset".into()));
        }
        let group = String::from_utf8(buf.split_to(group_len as usize).to_vec())
            .map_err(|_|ProtocolError::PayloadError("Invalid UTF-8 in group".into()))?;
        let offset = buf.get_u64();
        
        Ok(Self{
            topic,
            partition,
            group,
            offset,
        })
        
    }
}