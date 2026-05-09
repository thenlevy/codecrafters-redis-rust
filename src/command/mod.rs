//! Command-line parsing: RESP tokens after decoding, derived syntax specs, and dispatch.

pub mod parse_errors;
pub mod spec_parse;

use command_spec_derive::{CommandSpec, OptionGroupSpec};

use crate::storage::{PushKind, PushOperation, RangeOperation, SetOperation};

use {
    bytes::Bytes,
    chrono::{Duration, Utc},
};

use crate::RedisValue;

use spec_parse::CommandSyntax;

#[derive(Debug, Clone, Copy)]
pub struct ParsedTail<'a>(pub &'a [Bytes]);

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("invalid command: {0}")]
    InvalidCommand(&'static str),
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

// --- Parsed shapes (syntax diagrams) ----------------------------------------

#[derive(CommandSpec)]
#[command_spec(name = "PING", ignore_remaining)]
pub struct PingParsed;

#[derive(CommandSpec)]
#[command_spec(name = "ECHO", exact_tail_tokens = 1)]
pub struct EchoParsed {
    #[positional(cardinality = exactly_one, utf8_echo)]
    pub message: Bytes,
}

#[derive(CommandSpec)]
#[command_spec(name = "GET", exact_tail_tokens = 1)]
pub struct GetParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
}

#[derive(OptionGroupSpec)]
pub enum SetExpiration {
    #[option_spec(absent)]
    None,
    #[option_spec(keyword = "EX")]
    Ex(Bytes),
    #[option_spec(keyword = "PX")]
    Px(Bytes),
}

#[derive(CommandSpec)]
#[command_spec(name = "SET")]
pub struct SetParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
    #[positional(cardinality = exactly_one)]
    pub value: Bytes,
    #[command_spec(option_group)]
    pub expiration: SetExpiration,
}

#[derive(CommandSpec)]
#[command_spec(name = "RPUSH")]
pub struct RPushParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
    #[positional(cardinality = one_or_many)]
    pub values: Vec<Bytes>,
}

#[derive(CommandSpec)]
#[command_spec(name = "LPUSH")]
pub struct LPushParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
    #[positional(cardinality = one_or_many)]
    pub values: Vec<Bytes>,
}

#[derive(CommandSpec)]
#[command_spec(name = "LRANGE", exact_tail_tokens = 3)]
pub struct LRangeParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
    #[positional(cardinality = exactly_one)]
    pub start: isize,
    #[positional(cardinality = exactly_one)]
    pub stop: isize,
}

impl TryFrom<SetParsed> for SetOperation {
    type Error = CommandError;

    fn try_from(s: SetParsed) -> Result<Self, Self::Error> {
        let expiration_ms = match s.expiration {
            SetExpiration::None => None,
            SetExpiration::Ex(b) => Some(parse_ttl_ms(b, 1000)?),
            SetExpiration::Px(b) => Some(parse_ttl_ms(b, 1)?),
        };

        Ok(SetOperation {
            key: s.key,
            value: s.value,
            expiration: expiration_ms.map(|ms| Utc::now() + Duration::milliseconds(ms)),
        })
    }
}

fn parse_ttl_ms(b: Bytes, mult: i64) -> Result<i64, CommandError> {
    use parse_errors::*;
    str::from_utf8(b.as_ref())
        .map_err(|_| CommandError::InvalidArgument(INVALID_UTF8))?
        .parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument(INVALID_NUMBER))
        .map(|v| v * mult)
}

impl From<LPushParsed> for PushOperation {
    fn from(p: LPushParsed) -> Self {
        PushOperation {
            kind: PushKind::LPush,
            key: p.key,
            values: p.values,
        }
    }
}

impl From<RPushParsed> for PushOperation {
    fn from(p: RPushParsed) -> Self {
        PushOperation {
            kind: PushKind::RPush,
            key: p.key,
            values: p.values,
        }
    }
}

impl From<LRangeParsed> for RangeOperation {
    fn from(p: LRangeParsed) -> Self {
        RangeOperation {
            key: p.key,
            start: p.start,
            end: p.stop,
        }
    }
}

#[derive(CommandSpec)]
#[command_spec(name = "LLEN", exact_tail_tokens = 1)]
pub struct LLenParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
}

#[derive(CommandSpec)]
#[command_spec(name = "LPOP", exact_tail_tokens = 1)]
pub struct LPopParsed {
    #[positional(cardinality = exactly_one)]
    pub key: Bytes,
}

pub enum Command {
    Ping,
    Echo(Bytes),
    Set(SetOperation),
    Push(PushOperation),
    Get(Bytes),
    Lrange(RangeOperation),
    Llen(Bytes),
    Lpop(Bytes),
    NoOp,
}

pub fn parse(words: &[Bytes]) -> Result<Command, CommandError> {
    if words.is_empty() {
        return Ok(Command::NoOp);
    }

    let first_bytes = words[0].as_ref();
    let command = String::from_utf8_lossy(first_bytes);
    println!("command: {command}");

    let tail = ParsedTail(&words[1..]);

    match command.as_ref() {
        "PING" => {
            PingParsed::try_from_tail(tail)?;
            Ok(Command::Ping)
        }
        "ECHO" => {
            let p = EchoParsed::try_from_tail(tail)?;
            Ok(Command::Echo(p.message))
        }
        "SET" => {
            let p = SetParsed::try_from_tail(tail)?;
            Ok(Command::Set(p.try_into()?))
        }
        "LPUSH" => {
            let p = LPushParsed::try_from_tail(tail)?;
            Ok(Command::Push(p.into()))
        }
        "RPUSH" => {
            let p = RPushParsed::try_from_tail(tail)?;
            Ok(Command::Push(p.into()))
        }
        "GET" => {
            let p = GetParsed::try_from_tail(tail)?;
            Ok(Command::Get(p.key))
        }
        "LRANGE" => {
            let p = LRangeParsed::try_from_tail(tail)?;
            Ok(Command::Lrange(p.into()))
        }
        "LLEN" => {
            let p = LLenParsed::try_from_tail(tail)?;
            Ok(Command::Llen(p.key))
        }
        "LPOP" => {
            let p = LPopParsed::try_from_tail(tail)?;
            Ok(Command::Lpop(p.key))
        }
        _ => Err(CommandError::InvalidCommand(
            parse_errors::UNKNOWN_COMMAND_WORD,
        )),
    }
}

pub fn normalize_command_args(value: &RedisValue) -> Result<Vec<Bytes>, CommandError> {
    match value {
        RedisValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RedisValue::BulkString(b) => out.push(b.clone()),
                    RedisValue::SimpleString(s) => out.push(s.clone()),
                    _ => {
                        return Err(CommandError::InvalidArgument(
                            parse_errors::RESPONSE_NOT_STRING_ELEMENTS,
                        ));
                    }
                }
            }
            Ok(out)
        }
        RedisValue::SimpleString(line) => {
            let line = line.as_ref();
            let mut out = Vec::new();
            let mut token_start = None::<usize>;
            for (i, &b) in line.iter().enumerate() {
                if b.is_ascii_whitespace() {
                    if let Some(s) = token_start {
                        out.push(Bytes::copy_from_slice(&line[s..i]));
                        token_start = None;
                    }
                } else if token_start.is_none() {
                    token_start = Some(i);
                }
            }
            if let Some(s) = token_start {
                out.push(Bytes::copy_from_slice(&line[s..]));
            }
            Ok(out)
        }
        _ => Err(CommandError::InvalidCommand(parse_errors::TOP_LEVEL_SHAPE)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn ping_ignores_trailing_tokens() {
        let words = vec![bs("PING"), bs("extra")];
        parse(&words).unwrap();
        let tail = ParsedTail(&words[1..]);
        PingParsed::try_from_tail(tail).unwrap();
    }

    #[test]
    fn set_with_ex() {
        let words = vec![bs("SET"), bs("k"), bs("v"), bs("EX"), bs("10")];
        parse(&words).unwrap();
    }

    #[test]
    fn set_bad_third_token() {
        let words = vec![bs("SET"), bs("k"), bs("v"), bs("NOPE")];
        match parse(&words) {
            Err(e) => {
                assert!(
                    e.to_string().contains(parse_errors::INVALID_OPTION_TOKEN),
                    "{e}"
                );
            }
            Ok(_) => panic!("unexpected ok"),
        }
    }
}
