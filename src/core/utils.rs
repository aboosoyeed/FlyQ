use crate::core::message::DeserializeError;
use xxhash_rust::xxh3::xxh3_64;

pub fn read_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8], DeserializeError> {
    if buf.len() < len {
        return Err(DeserializeError::UnexpectedEOF);
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    Ok(head)
}

pub fn hash_key_to_partition(key: &[u8], num_partitions: u32) -> u32 {
    let hash = xxh3_64(key);
    (hash as u32) % num_partitions
}
