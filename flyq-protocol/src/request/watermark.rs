use crate::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct WatermarkRequest {
    pub topic: String,
    pub partition: u32,
}

impl WatermarkRequest {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.topic.len() as u32);
        buf.extend_from_slice(self.topic.as_bytes());
        buf.put_u32(self.partition);
        buf.freeze()
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for topic length".into(),
            ));
        }
        let topic_len = buf.get_u32();

        if buf.remaining() < (topic_len + 4) as usize {
            return Err(ProtocolError::PayloadError(
                "Insufficient data for topic + partition".into(),
            ));
        }
        let topic = String::from_utf8(buf.split_to(topic_len as usize).to_vec())
            .map_err(|_| ProtocolError::PayloadError("Invalid UTF-8 in topic".into()))?;

        let partition = buf.get_u32();

        Ok(Self { topic, partition })
    }
}
