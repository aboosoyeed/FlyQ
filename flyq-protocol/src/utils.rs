use crate::error::DeserializeError;

pub fn read_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8], DeserializeError> {
    if buf.len() < len {
        return Err(DeserializeError::UnexpectedEOF);
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    Ok(head)
}