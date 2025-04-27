use bytes::{Bytes, BytesMut, Buf, BufMut};
use crate::errors::ProtocolError;

#[derive(Debug)]
pub struct ProduceAck {
    pub partition: u32,
    pub offset: u64,
}

impl ProduceAck {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(12);
        buf.put_u32(self.partition);
        buf.put_u64(self.offset);
        buf.freeze()
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 12 {
            return Err(ProtocolError::PayloadError("Incomplete produce ack payload".into()));
        }
        let partition = buf.get_u32();
        let offset = buf.get_u64();

        Ok(ProduceAck { partition, offset })
    }
}
