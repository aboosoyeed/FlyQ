use crate::ProtocolError;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum OpCode {
    Produce = 1,
    Consume = 2,
    ConsumeWithGroup = 3,
    CommitOffset = 4,
    Watermark = 5,
    GetConsumerLag = 13,
    GetPartitionHealth = 14,
}

impl TryFrom<u8> for OpCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OpCode::Produce),
            2 => Ok(OpCode::Consume),
            3 => Ok(OpCode::ConsumeWithGroup),
            4 => Ok(OpCode::CommitOffset),
            5 => Ok(OpCode::Watermark),
            13 => Ok(OpCode::GetConsumerLag),
            14 => Ok(OpCode::GetPartitionHealth),
            _ => Err(ProtocolError::UnknownOpCode(value)),
        }
    }
}
