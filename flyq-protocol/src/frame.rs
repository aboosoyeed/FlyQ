/*
[ version: u8 ]
[ frame_type: u8 ]
[ correlation_id: u32 ]
[ payload_len: u32 ]
[checksum : u32]
[ payload bytes... ]
*/

use bytes::{Buf, BufMut, BytesMut};
use xxhash_rust::xxh32::xxh32;
use crate::{ProtocolError};

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FrameType {
    Request = 1,
    Response = 2,
    Error = 3,
    Heartbeat = 4, // (future: keepalive)
}

impl TryFrom<u8> for FrameType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(FrameType::Request),
            2 => Ok(FrameType::Response),
            3 => Ok(FrameType::Error),
            4 => Ok(FrameType::Heartbeat),
            _ => Err(ProtocolError::UnknownFrameType(value)),
        }
    }
}

#[derive(Debug)]
pub struct Frame {
    pub version: u8,          // Protocol version
    pub frame_type: FrameType, // Request, Response, Error
    pub correlation_id: u32,  // Matches request to response
    pub payload: Vec<u8>,      // Raw payload
}

impl Frame {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.frame_type as u8);
        buf.put_u32(self.correlation_id);
        buf.put_u32(self.payload.len() as u32);
        let hash = xxh32(&self.payload, 0);
        buf.put_u32(hash);
        buf.extend_from_slice(&self.payload);
    }

    pub fn decode(buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
        if buf.len() < 10 {
            return Ok(None); // Not enough for frame header
        }

        let mut cursor = &buf[..];

        let version = cursor.get_u8();
        let frame_type_raw = cursor.get_u8();
        let correlation_id = cursor.get_u32();
        let payload_len = cursor.get_u32() as usize;
        let checksum_expected = cursor.get_u32();
        
        if cursor.remaining() < payload_len {
            return Ok(None); // Payload not fully available yet
        }

        // At this point, full frame is available
        //let total_len = 10 + payload_len;
        buf.advance(10); // consume header
        let payload = buf.split_to(payload_len).to_vec();
        let checksum_actual = xxh32(&payload,0);

        if checksum_actual != checksum_expected {
            return Err(ProtocolError::ChecksumMismatch {
                expected: checksum_expected,
                found: checksum_actual,
            });
        }
        
        Ok(Some(Frame {
            version,
            frame_type: FrameType::try_from(frame_type_raw)?,
            correlation_id,
            payload,
        }))
    }



}