use bytes::{Bytes, BytesMut, Buf, BufMut};
use crate::error::ProtocolError;

#[derive(Debug)]
pub struct ProduceRequest {
    pub topic: String,
    pub message: Bytes,
}

impl ProduceRequest {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.topic.len() as u32);
        buf.extend_from_slice(self.topic.as_bytes());
        buf.put_u32(self.message.len() as u32);
        buf.extend_from_slice(&self.message);
        buf.freeze()
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError("Incomplete produce payload".into()));
        }
        let topic_len = buf.get_u32() as usize;
        if buf.remaining() < topic_len + 4 {
            return Err(ProtocolError::PayloadError("Incomplete produce payload".into()));
        }
        let topic_bytes = buf.split_to(topic_len);
        let topic = String::from_utf8(topic_bytes.to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic"))?;
        let message_len = buf.get_u32() as usize;
        if buf.remaining() < message_len {
            return Err(ProtocolError::PayloadError("Incomplete message payload".into()));
        }
        let message = buf.split_to(message_len);

        Ok(ProduceRequest { topic, message })
    }
}
