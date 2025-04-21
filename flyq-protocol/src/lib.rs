// flyq-protocol/src/lib.rs

use thiserror::Error;
use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum OpCode {
    Produce = 1,
    Consume = 2,
}

impl TryFrom<u8> for OpCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OpCode::Produce),
            2 => Ok(OpCode::Consume),
            _ => Err(ProtocolError::UnknownOpCode(value)),
        }
    }
}

#[derive(Debug)]
pub struct Frame {
    pub op: OpCode,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.op as u8);
        buf.put_u32(self.payload.len() as u32);
        buf.extend_from_slice(&self.payload);
    }

    pub fn decode(buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if buf.len() < 5 {
            return Ok(None); // not enough data yet
        }

        let op = OpCode::try_from(buf[0])?;
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        if buf.len() < 5 + len {
            return Ok(None); // not enough payload yet
        }

        buf.advance(5);
        let payload = buf.split_to(len).to_vec();

        Ok(Some(Frame { op, payload }))
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Unknown opcode: {0}")]
    UnknownOpCode(u8),

    #[error("Incomplete frame")]
    IncompleteFrame,

    #[error("Payload decode error: {0}")]
    PayloadError(String),
}
