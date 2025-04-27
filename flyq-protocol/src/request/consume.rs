use bytes::{Bytes, Buf};
use crate::errors::ProtocolError;

#[derive(Debug)]
pub struct ConsumeRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

impl ConsumeRequest {
    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError("Incomplete consume payload".into()));
        }
        let topic_len = buf.get_u32() as usize;
        if buf.remaining() < topic_len + 12 {
            return Err(ProtocolError::PayloadError("Incomplete consume payload".into()));
        }
        let topic_bytes = buf.split_to(topic_len);
        let topic = String::from_utf8(topic_bytes.to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;
        let partition = buf.get_u32();
        let offset = buf.get_u64();

        Ok(ConsumeRequest { topic, partition, offset })
    }
}
