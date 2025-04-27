use crate::ProtocolError;

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