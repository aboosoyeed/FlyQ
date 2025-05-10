/*
[ message_length : u32 ]
[ offset         : u64 ]
[ timestamp      : u64 ]
[ key_len        : u32 ]
[ key bytes      : [u8] ]
[ value_len      : u32 ]
[ value bytes    : [u8] ]
[ header_count   : u32 ]
[ headers: (key_len, key, val_len, val)* ]

*/
use bytes::Bytes;
use crate::errors::DeserializeError;
use crate::utils::read_bytes;

#[derive(Debug, Clone)]
pub struct Message {
    pub key: Option<Vec<u8>>, // Optional message key (used for partitioning)
    pub value: Vec<u8>,       // Message body
    pub timestamp: u64,       // Unix epoch in millis
    pub headers: Option<Vec<(String, Vec<u8>)>>, // Optional headers (Kafka-style)
}

impl Message {
    pub fn serialize_for_disk(&self, offset: u64) -> Vec<u8> {
        let mut buf = Vec::new();

        // Reserve space to write length later
        buf.extend_from_slice(&[0u8; 4]); // placeholder

        buf.extend(offset.to_be_bytes());
        buf.extend(&self.timestamp.to_be_bytes());

        // Key
        if let Some(ref key) = &self.key {
            buf.extend(&(key.len() as u32).to_be_bytes());
            buf.extend(key);
        } else {
            buf.extend(&0u32.to_be_bytes());
        }

        // Value
        buf.extend(&(self.value.len() as u32).to_be_bytes());
        buf.extend(&self.value);

        // Headers
        if let Some(ref headers) = &self.headers {
            buf.extend(&(headers.len() as u32).to_be_bytes());
            for (k, v) in headers {
                buf.extend(&(k.len() as u32).to_be_bytes());
                buf.extend(k.as_bytes());

                buf.extend(&(v.len() as u32).to_be_bytes());
                buf.extend(v);
            }
        } else {
            buf.extend(&0u32.to_be_bytes());
        }

        // Now write message length at start
        let msg_len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&msg_len.to_be_bytes());

        buf
    }

    /// Used for sending over the network — does not include `[len]`.
    pub fn serialize_for_wire(&self, offset: u64) -> Bytes {
        let raw = self.serialize_for_disk(offset);
        let len = u32::from_be_bytes(raw[0..4].try_into().unwrap()) as usize;
        Bytes::copy_from_slice(&raw[4..4 + len])
    }

    pub fn deserialize(mut buf: &[u8]) -> Result<(u64, Message), DeserializeError> {
        // Offset
        let offset = {
            let b = read_bytes(&mut buf, 8)?;
            u64::from_be_bytes(b.try_into().unwrap())
        };

        // Timestamp
        let timestamp = {
            let b = read_bytes(&mut buf, 8)?;
            u64::from_be_bytes(b.try_into().unwrap())
        };

        // Key
        let key_len = {
            let b = read_bytes(&mut buf, 4)?;
            u32::from_be_bytes(b.try_into().unwrap()) as usize
        };

        let key = if key_len > 0 {
            Some(read_bytes(&mut buf, key_len)?.to_vec())
        } else {
            None
        };

        // Value
        let value_len = {
            let b = read_bytes(&mut buf, 4)?;
            u32::from_be_bytes(b.try_into().unwrap()) as usize
        };

        let value = read_bytes(&mut buf, value_len)?.to_vec();

        // Headers
        let header_count = {
            let b = read_bytes(&mut buf, 4)?;
            u32::from_be_bytes(b.try_into().unwrap()) as usize
        };

        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let k_len = u32::from_be_bytes(read_bytes(&mut buf, 4)?.try_into().unwrap()) as usize;
            let k = String::from_utf8(read_bytes(&mut buf, k_len)?.to_vec())
                .map_err(|_| DeserializeError::InvalidUtf8)?;

            let v_len = u32::from_be_bytes(read_bytes(&mut buf, 4)?.try_into().unwrap()) as usize;
            let v = read_bytes(&mut buf, v_len)?.to_vec();

            headers.push((k, v));
        }

        Ok((
            offset,
            Message {
                key,
                value,
                timestamp,
                headers: if headers.is_empty() {
                    None
                } else {
                    Some(headers)
                },
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = Message {
            key: Some(b"user-42".to_vec()),
            value: b"click:event".to_vec(),
            timestamp: 1700000000000,
            headers: Some(vec![
                ("event-type".to_string(), b"click".to_vec()),
                ("source".to_string(), b"web".to_vec()),
            ]),
        };

        let offset = 12345;
        let serialized = original.serialize_for_disk(offset);

        // Extract message length and slice buffer
        let msg_len = u32::from_be_bytes(serialized[0..4].try_into().unwrap()) as usize;
        let msg_buf = &serialized[4..4 + msg_len];

        let (parsed_offset, parsed_msg) =
            Message::deserialize(msg_buf).expect("deserialize failed");

        assert_eq!(parsed_offset, offset);
        assert_eq!(parsed_msg.timestamp, original.timestamp);
        assert_eq!(parsed_msg.key, original.key);
        assert_eq!(parsed_msg.value, original.value);
        assert_eq!(parsed_msg.headers, original.headers);
    }

    #[test]
    fn test_deserialize_with_no_key_or_headers() {
        let msg = Message {
            key: None,
            value: b"just value".to_vec(),
            timestamp: 42,
            headers: None,
        };

        let serialized = msg.serialize_for_disk(1);
        // Extract message length and slice buffer
        let msg_len = u32::from_be_bytes(serialized[0..4].try_into().unwrap()) as usize;
        let msg_buf = &serialized[4..4 + msg_len];

        let (_, deserialized) = Message::deserialize(msg_buf).unwrap();
        assert_eq!(deserialized.key, None);
        assert_eq!(deserialized.headers, None);
        assert_eq!(deserialized.value, msg.value);
        assert_eq!(deserialized.timestamp, 42);
    }
}
