//! RESP (Redis Serialization Protocol) parsing.
//!
//! Streaming parser patterned after [`BufReader`]: one growable byte buffer plus a consume cursor,
//! refilled from an [`AsyncRead`] until enough bytes exist to form a [`RespValue`].

// See https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators/

use super::RedisValue;

use {
    bytes::{Bytes, BytesMut},
    tokio_util::codec::{Decoder, Encoder},
};

#[derive(Default)]
pub struct RespParser;

impl Decoder for RespParser {
    type Item = RedisValue;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some((pos, value)) = parse(src)? else {
            return Ok(None);
        };

        let our_data = src.split_to(pos).freeze();

        Ok(Some(value.redis_value(&our_data)))
    }
}

impl Encoder<RedisValue> for RespParser {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RedisValue::Integer(i) => {
                dst.extend_from_slice(format!("-{i}\r\n").as_bytes());
            }
            RedisValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RedisValue::BulkString(s) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(s.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&s);
                dst.extend_from_slice(b"\r\n");
            }
            RedisValue::Array(a) => {
                dst.extend_from_slice(format!("*{}\r\n", a.len()).as_bytes());
                for value in a {
                    self.encode(value, dst)?;
                }
            }
            RedisValue::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RedisValue::Error(e) => {
                dst.extend_from_slice(format!("-{e}\r\n").as_bytes());
            }
        }

        Ok(())
    }
}

impl RespBufSplit {
    fn redis_value(&self, buf: &Bytes) -> RedisValue {
        match self {
            RespBufSplit::Int(i) => RedisValue::Integer(*i),
            RespBufSplit::SimpleString(split) => RedisValue::SimpleString(split.split_bytes(buf)),
            RespBufSplit::BulkString(split) => RedisValue::BulkString(split.split_bytes(buf)),
            RespBufSplit::Array(values) => {
                RedisValue::Array(values.into_iter().map(|v| v.redis_value(buf)).collect())
            }
            RespBufSplit::NullArray | RespBufSplit::NullString => RedisValue::Null,
        }
    }
}

struct BufSplit(usize, usize);

const CRLF: &[u8] = b"\r\n";

impl BufSplit {
    fn slice_bytes<'a>(&'a self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    fn split_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

type RespResult = Result<Option<(usize, RespBufSplit)>, RespError>;

enum RespBufSplit {
    BulkString(BufSplit),
    SimpleString(BufSplit),
    Array(Vec<RespBufSplit>),
    Int(i64),
    /// None for an Optional array
    NullArray,
    /// None for an Optional string
    NullString,
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("invalid int")]
    InvalidInt,
    #[error("invalid simple string: {0}")]
    InvalidSimpleString(&'static str),
    #[error("invalid bulk string: {0}")]
    InvalidBulkString(&'static str),
    #[error("invalid array: {0}")]
    InvalidArray(&'static str),
    #[error("internal error: {0}")]
    InternalError(&'static str),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

fn parse(buf: &BytesMut) -> RespResult {
    println!("entering parse {buf:?}");
    if buf.len() == 0 {
        return Ok(None);
    }

    if buf[0] == b'*' {
        array(buf, 1)
    } else {
        // Handle inline command
        simple_string(buf, 0)
    }
}

/// Find a CRLF ending word
/// Returns None if no CRLF is found (more bytes are needed)
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if pos >= buf.len() {
        return None;
    }
    memchr::memmem::find(&buf[pos..], CRLF).map(|idx| (pos + idx + 2, BufSplit(pos, pos + idx)))
}

fn int(buf: &BytesMut, pos: usize) -> RespResult {
    let Some((end_position, split)) = word(buf, pos) else {
        return Ok(None);
    };

    let bytes = split.slice_bytes(buf);

    let (sign, start) = if bytes[0] == b'-' { (-1, 1) } else { (1, 0) };

    let mut ret = 0;

    for b in &bytes[start..] {
        if *b < b'0' || *b > b'9' {
            return Err(RespError::InvalidInt);
        }
        ret = ret * 10 + (*b - b'0') as i64;
    }

    Ok(Some((end_position, RespBufSplit::Int(sign * ret))))
}

fn simple_string(buf: &BytesMut, pos: usize) -> RespResult {
    let Some((end_position, split)) = word(buf, pos) else {
        return Ok(None);
    };

    let bytes = split.slice_bytes(buf);

    if memchr::memchr(b'\r', bytes).is_some() || memchr::memchr(b'\n', bytes).is_some() {
        return Err(RespError::InvalidSimpleString(
            r"simple string contains '\r' or '\n'",
        ));
    }

    Ok(Some((end_position, RespBufSplit::SimpleString(split))))
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RespResult {
    let Some((string_start, string_length)) = int(buf, pos)? else {
        return Ok(None);
    };

    let RespBufSplit::Int(string_length) = string_length else {
        return Err(RespError::InternalError("string length is not an integer"));
    };

    if string_length < -1 {
        return Err(RespError::InvalidBulkString(
            "bulk string length is less than -1",
        ));
    }

    if string_length == -1 {
        return Ok(Some((string_start, RespBufSplit::NullString)));
    }

    let string_length = string_length.max(0) as usize;

    let crlf_position = string_start + string_length;

    if crlf_position + CRLF.len() > buf.len() {
        return Ok(None);
    }

    if &buf[crlf_position..crlf_position + CRLF.len()] != CRLF {
        return Err(RespError::InvalidBulkString(
            "Expected CRLF after bulk string",
        ));
    }

    Ok(Some((
        crlf_position + CRLF.len(),
        RespBufSplit::BulkString(BufSplit(string_start, crlf_position)),
    )))
}

fn array(buf: &BytesMut, mut pos: usize) -> RespResult {
    let Some((array_start, array_length)) = int(buf, pos)? else {
        return Ok(None);
    };

    let RespBufSplit::Int(array_length) = array_length else {
        return Err(RespError::InternalError("array length is not an integer"));
    };

    if array_length < -1 {
        return Err(RespError::InvalidArray("array length is less than -1"));
    }

    if array_length == -1 {
        return Ok(Some((array_start, RespBufSplit::NullArray)));
    }

    let array_length = array_length.max(0) as usize;

    let mut ret = Vec::with_capacity(array_length);

    pos = array_start;

    for _ in 0..array_length {
        let Some((new_pos, value)) = inner_parse(buf, pos)? else {
            return Ok(None);
        };
        pos = new_pos;
        ret.push(value);
    }

    Ok(Some((pos, RespBufSplit::Array(ret))))
}

fn inner_parse(buf: &BytesMut, pos: usize) -> RespResult {
    if pos >= buf.len() {
        return Ok(None);
    }

    match buf[pos] {
        b'*' => array(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b'+' => simple_string(buf, pos + 1),
        b':' => int(buf, pos + 1),
        c => {
            println!("invalid character: {}", c as char);
            Err(RespError::InternalError(
                "invalid character, or unhandeled type",
            ))
        }
    }
}
