use bytes::{Bytes, BytesMut, Buf, BufMut};
use crate::errors::ProtocolError;
use crate::op_code::OpCode;

#[derive(Debug)]
pub struct RequestPayload {
    pub op_code: OpCode,
    pub data: Bytes, // ⚡ Now efficient zero-copy buffer
}

impl RequestPayload {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + self.data.len());
        buf.put_u8(self.op_code as u8);
        buf.extend_from_slice(&self.data);
        buf.freeze() // Turns BytesMut -> Bytes
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::PayloadError("Empty request payload".into()));
        }

        let op_code = OpCode::try_from(buf.get_u8())?;
        let data = buf; // Remaining bytes are the data

        Ok(RequestPayload { op_code, data })
    }
}

#[derive(Debug)]
pub struct ResponsePayload {
    pub op_code: OpCode,
    pub data: Bytes,
}

impl ResponsePayload {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + self.data.len());
        buf.put_u8(self.op_code as u8);
        buf.extend_from_slice(&self.data);
        buf.freeze()
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::PayloadError("Empty response payload".into()));
        }

        let op_code = OpCode::try_from(buf.get_u8())?;
        let data = buf;
        Ok(ResponsePayload { op_code, data })
    }
}
    
