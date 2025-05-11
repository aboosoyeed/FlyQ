use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::message::Message;
use crate::errors::ProtocolError;

#[derive(Debug)]
pub struct ConsumeResponse {
    pub offset: u64,
    pub message: Message,
}

impl ConsumeResponse {
    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.len() < 8{
            return Err(ProtocolError::PayloadError("Payload too short for offset".into()));
        }
        let offset = buf.get_u64(); // read
        let message = Message::deserialize_body(&buf)?;
        Ok(ConsumeResponse { offset, message })
    }

    pub fn serialize(&self) -> Bytes {

        //8 (timestamp) +
        // 4 (key len) +
        // 4 (value len) +
        // 4 (header count) +
        // ~12 for small strings or rounding

        let mut buf = BytesMut::with_capacity(8 + self.message.value.len() + 32); // rough guess
        buf.put_u64(self.offset); // prefix offset
        buf.extend_from_slice(&self.message.serialize_for_wire()); // clean message
        buf.freeze()
    }
}
