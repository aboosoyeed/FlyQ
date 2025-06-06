use crate::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct WatermarkResponse {
    pub log_end_offset: u64,
    pub low_watermark: u64,
    pub high_watermark: u64,
}

impl WatermarkResponse {
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u64(self.low_watermark);
        buf.put_u64(self.high_watermark);
        buf.put_u64(self.log_end_offset);
        buf.freeze()
    }

    pub fn deserialize(mut buf: Bytes) -> Result<Self, ProtocolError> {
        if buf.remaining() < 24 {
            return Err(ProtocolError::PayloadError(
                "Incomplete watermark response payload".into(),
            ));
        }
        let low_watermark = buf.get_u64();
        let high_watermark = buf.get_u64();
        let log_end_offset = buf.get_u64();

        Ok(Self {
            log_end_offset,
            low_watermark,
            high_watermark,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::WatermarkResponse;

    #[test]
    fn test_watermark_roundtrip() {
        let original = WatermarkResponse {
            low_watermark: 1,
            high_watermark: 2,
            log_end_offset: 3,
        };

        let bytes = original.serialize();
        let parsed = WatermarkResponse::deserialize(bytes).unwrap();

        assert_eq!(original.low_watermark, parsed.low_watermark);
        assert_eq!(original.high_watermark, parsed.high_watermark);
        assert_eq!(original.log_end_offset, parsed.log_end_offset);
    }
}
