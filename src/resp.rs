//! RESP (Redis Serialization Protocol) parsing.
//!
//! Streaming parser patterned after [`BufReader`]: one growable byte buffer plus a consume cursor,
//! refilled from an [`AsyncRead`] until enough bytes exist to form a [`RespValue`].

use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub struct RespParser<R> {
    reader: R,
    /// All bytes read from `reader`; meaningful unconsumed range starts at `consume`.
    buf: Vec<u8>,
    /// Index of the first byte not yet consumed by parsing.
    consume: usize,
    eof: bool,
}

pub enum RawCommand {
    Inlined(String),
    Bulk(Vec<String>),
}

impl<'l> From<&'l RawCommand> for Command<'l> {
    fn from(raw: &'l RawCommand) -> Self {
        match raw {
            RawCommand::Inlined(line) => Command::from_inline(line),
            RawCommand::Bulk(command) => Command::from_bulk(command),
        }
    }
}

pub enum Command<'l> {
    Ping,
    Echo(&'l str),
    EchoOwned(&'l [String]),
    Unknown(&'l str),
    Empty,
}

impl<'l> Command<'l> {
    fn from_inline(line: &'l str) -> Self {
        let mut words = line.trim().split_whitespace();

        let Some(verb) = words.next() else {
            return Command::Empty;
        };

        match verb {
            "PING" => Command::Ping,
            "ECHO" => {
                let Some((_echo, arg)) = line.trim().split_once(' ') else {
                    return Command::Echo("");
                };
                Command::Echo(arg)
            }
            _ => Command::Unknown(line),
        }
    }

    fn from_bulk(command: &'l Vec<String>) -> Self {
        let Some(verb) = command.first() else {
            return Command::Empty;
        };

        match verb.as_str() {
            "PING" => Command::Ping,
            "ECHO" => Command::EchoOwned(&command[1..]),
            _ => Command::Unknown(verb.as_str()),
        }
    }
}

impl<R> RespParser<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            consume: 0,
            eof: false,
        }
    }

    #[inline]
    fn remaining(&self) -> &[u8] {
        &self.buf[self.consume..]
    }

    fn finish_consumption(&mut self) {
        if self.consume == 0 {
            return;
        }
        if self.consume >= self.buf.len() {
            self.buf.clear();
            self.consume = 0;
            return;
        }
        const COMPACT_AT: usize = 4096;
        if self.consume >= COMPACT_AT {
            self.buf.drain(..self.consume);
            self.consume = 0;
        }
    }

    #[inline]
    fn advance(&mut self, n: usize) {
        self.consume += n;
        self.finish_consumption();
    }

    /// Reserve capacity so refilling can store the bulk payload without reallocating for each
    /// chunk.
    ///
    /// Call immediately after parsing the bulk length line; `remaining()` is then the start of the
    /// payload (possibly empty if the next bytes have not arrived yet).
    fn reserve_for_bulk_payload(&mut self, payload_len: usize) {
        let already_buffered = self.remaining().len();
        let need_from_reader = payload_len.saturating_sub(already_buffered);
        self.buf.reserve(need_from_reader);
    }
}

impl<R: AsyncRead + Unpin> RespParser<R> {
    async fn refill(&mut self) -> Result<usize, RespError> {
        if self.eof {
            return Ok(0);
        }
        const CHUNK: usize = 8192;
        let start = self.buf.len();
        self.buf.reserve(CHUNK);
        self.buf.resize(start + CHUNK, 0);
        let n = self
            .reader
            .read(&mut self.buf[start..])
            .await
            .map_err(RespError::Io)?;
        self.buf.truncate(start + n);
        if n == 0 {
            self.eof = true;
        }
        Ok(n)
    }

    /// Reads a line up to and excluding `\r\n`, advances past `\r\n`.
    async fn read_line_bytes(&mut self) -> Result<Vec<u8>, RespError> {
        loop {
            if let Some(rel) = find_crlf(self.remaining()) {
                let line = self.remaining()[..rel].to_vec();
                self.advance(rel + 2);
                return Ok(line);
            }

            let n = self.refill().await?;
            if n == 0 {
                return if self.remaining().is_empty() {
                    Err(RespError::Incomplete)
                } else {
                    Err(RespError::ExpectedCrlf)
                };
            }
        }
    }

    async fn parse_int_line(&mut self) -> Result<isize, RespError> {
        let line = self.read_line_bytes().await?;
        let s = std::str::from_utf8(&line).map_err(|_| RespError::InvalidIntUtf8)?;
        Ok(s.parse()?)
    }

    /// Consume exactly `n` bytes from the stream (payload only; trailing CRLF is separate).
    async fn take_payload(&mut self, n: usize) -> Result<Vec<u8>, RespError> {
        let mut out = Vec::with_capacity(n);

        while out.len() < n {
            let need = n - out.len();
            let rem = self.remaining().len();

            if rem >= need {
                let start = self.consume;
                out.extend_from_slice(&self.buf[start..start + need]);
                self.advance(need);
                continue;
            }

            if rem != 0 {
                out.extend_from_slice(self.remaining());
                self.advance(rem);
                continue;
            }

            let read = self.refill().await?;
            if read == 0 {
                return Err(RespError::Incomplete);
            }
        }

        Ok(out)
    }

    #[allow(unused)]
    pub async fn next_value(&mut self) -> Result<RespValue, RespError> {
        self.parse_value().await
    }

    pub async fn next_raw_command(&mut self) -> Result<Option<RawCommand>, CommandError> {
        loop {
            if let Some(&b) = self.remaining().first() {
                break if b == b'*' {
                    let array = self.parse_array().await?;
                    let strings = array
                        .into_iter()
                        .map(|v| match v {
                            RespValue::BulkString(s) => Ok(s),
                            _ => Err(CommandError::InvalidBulkString),
                        })
                        .collect::<Result<_, _>>()?;
                    Ok(Some(RawCommand::Bulk(strings)))
                } else {
                    loop {
                        if let Some(rel) = find_crlf(self.remaining()) {
                            let line = String::from_utf8(self.remaining()[..rel].to_vec())
                                .map_err(|_| RespError::InvalidIntUtf8)
                                .map_err(CommandError::Resp)?;
                            self.advance(rel + 2);
                            break Ok(Some(RawCommand::Inlined(line)));
                        } else if let Some(end) = self.remaining().iter().position(|b| *b == b'\n')
                        {
                            // LF without preceding CR (avoid leaving '\n' buffered → advance(0)
                            // loop)
                            let line = String::from_utf8(self.remaining()[..end].to_vec())
                                .map_err(|_| RespError::InvalidIntUtf8)
                                .map_err(CommandError::Resp)?;
                            self.advance(end + 1);
                            break Ok(Some(RawCommand::Inlined(line)));
                        }

                        let n = self.refill().await?;
                        if n == 0 {
                            return Err(CommandError::Resp(RespError::Incomplete));
                        }
                    }
                };
            }
            let n = self.refill().await?;
            if n == 0 {
                return Ok(None);
            }
        }
    }

    async fn parse_value(&mut self) -> Result<RespValue, RespError> {
        let peek = self.peek_byte().await?;

        match peek {
            b'$' => {
                let _ = self.take_byte().await?;
                Ok(RespValue::BulkString(self.parse_bulk_string().await?))
            }
            b'*' => {
                let _ = self.take_byte().await?;
                Ok(RespValue::Array(self.parse_array().await?))
            }
            b => Err(RespError::UnknownType(b)),
        }
    }

    async fn peek_byte(&mut self) -> Result<u8, RespError> {
        loop {
            if let Some(&b) = self.remaining().first() {
                return Ok(b);
            }
            let n = self.refill().await?;
            if n == 0 {
                return Err(RespError::Incomplete);
            }
        }
    }

    async fn take_byte(&mut self) -> Result<u8, RespError> {
        loop {
            if !self.remaining().is_empty() {
                let b = self.buf[self.consume];
                self.advance(1);
                return Ok(b);
            }
            let n = self.refill().await?;
            if n == 0 {
                return Err(RespError::Incomplete);
            }
        }
    }

    async fn skip_crlf(&mut self) -> Result<(), RespError> {
        let b = self.take_byte().await?;
        if b != b'\r' {
            return Err(RespError::ExpectedCrlf);
        }
        let b = self.take_byte().await?;
        if b != b'\n' {
            return Err(RespError::ExpectedCrlf);
        }
        Ok(())
    }

    /// `$<len>\r\n` then `len` bytes of payload, then `\r\n`. Null bulk: `$-1\r\n`.
    async fn parse_bulk_string(&mut self) -> Result<String, RespError> {
        let len = self.parse_int_line().await?;
        if len < 0 {
            return Ok(String::new());
        }
        let len = len as usize;
        self.reserve_for_bulk_payload(len);
        let bytes = self.take_payload(len).await?;
        self.skip_crlf().await?;
        String::from_utf8(bytes).map_err(|_| RespError::InvalidUtf8)
    }

    async fn parse_array(&mut self) -> Result<Vec<RespValue>, RespError> {
        let count = self.parse_int_line().await?;
        if count < 0 {
            return if count == -1 {
                Ok(vec![])
            } else {
                Err(RespError::NegativeArrayLength(count))
            };
        }

        let count = count as usize;
        let mut elements = Vec::with_capacity(count);
        for _ in 0..count {
            let v = Box::pin(self.parse_value()).await?;
            elements.push(v);
        }
        Ok(elements)
    }
}

/// Parsed RESP value; only bulk strings and arrays are supported (owned payloads).
#[derive(Debug, PartialEq, Eq)]
pub enum RespValue {
    BulkString(String),
    Array(Vec<RespValue>),
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("unexpected end of input")]
    Incomplete,
    #[error("expected CRLF")]
    ExpectedCrlf,
    #[error("invalid UTF-8 in integer line")]
    InvalidIntUtf8,
    #[error("invalid UTF-8 in bulk string")]
    InvalidUtf8,
    #[error("invalid integer: {0}")]
    InvalidInt(#[from] std::num::ParseIntError),
    #[error("unknown type prefix {0:?} (expected '$' or '*')")]
    UnknownType(u8),
    #[error("negative array length {0}")]
    NegativeArrayLength(isize),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("invalid bulk string")]
    InvalidBulkString,
    #[error("RESP error: {0}")]
    Resp(#[from] RespError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            io::Cursor,
            pin::Pin,
            task::{Context, Poll},
        },
        tokio::io::ReadBuf,
    };

    struct ChunkReader {
        chunks: std::vec::IntoIter<Vec<u8>>,
        current: Option<(Vec<u8>, usize)>,
    }

    impl ChunkReader {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks: chunks.into_iter(),
                current: None,
            }
        }
    }

    impl AsyncRead for ChunkReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            loop {
                if let Some((data, consumed)) = &mut self.current {
                    if *consumed >= data.len() {
                        self.current = None;
                        continue;
                    }
                    buf.put_slice(&data[*consumed..]);
                    *consumed = data.len();
                    return Poll::Ready(Ok(()));
                }
                match self.chunks.next() {
                    Some(next) => self.current = Some((next, 0)),
                    None => return Poll::Ready(Ok(())),
                }
            }
        }
    }

    #[tokio::test]
    async fn parses_ping_from_single_cursor() {
        let data = b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n".to_vec();
        let mut p = RespParser::new(Cursor::new(data));
        match p.next_value().await.unwrap() {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString("PING".into()));
                assert_eq!(arr[1], RespValue::BulkString("".into()));
            }
            other => panic!("expected array: {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_across_multiple_reads() {
        let chunks = vec![
            vec![b'*'],
            b"2\r".to_vec(),
            b"\n$4\r".to_vec(),
            b"\nPING\r\n$".to_vec(),
            b"0\r\n\r\n".to_vec(),
        ];
        let mut p = RespParser::new(ChunkReader::new(chunks));
        match p.next_value().await.unwrap() {
            RespValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString("PING".into()));
                assert_eq!(arr[1], RespValue::BulkString("".into()));
            }
            other => panic!("expected array: {other:?}"),
        }
    }
}
