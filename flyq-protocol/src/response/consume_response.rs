use bytes::{Bytes};
use crate::message::Message;
use crate::errors::ProtocolError;

#[derive(Debug)]
pub struct ConsumeResponse {
    pub offset: u64,
    pub message: Message,
}

impl ConsumeResponse {
    pub fn deserialize(buf: Bytes) -> Result<Self, ProtocolError> {
        let (offset, message) = Message::deserialize(&buf)?;
        Ok(ConsumeResponse { offset, message })
    }

    pub fn serialize(&self) -> Bytes {
        let buf = self.message.serialize(self.offset);  // pass correct offset
        Bytes::from(buf)
    }
}
