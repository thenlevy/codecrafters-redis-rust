//! Traits and helpers used by `command_spec_derive` generated parsers.

use bytes::Bytes;

use super::{CommandError, ParsedTail, parse_errors};

/// Implemented by `#[derive(CommandSpec)]` on the parsed struct shape.
#[allow(dead_code)]
pub trait CommandSyntax: Sized {
    #[allow(dead_code)]
    const COMMAND_NAME: &'static str;
    fn try_from_tail(parsed_tail: ParsedTail<'_>) -> Result<Self, CommandError>;
}

pub trait OptionGroupParser: Sized {
    fn parse_option_group(tokens: &[Bytes], idx: &mut usize) -> Result<Self, CommandError>;
}

/// Parse one token as UTF-8 then as `isize` (positional integer arguments).
pub fn parse_isize_token(tokens: &[Bytes], idx: &mut usize) -> Result<isize, CommandError> {
    if *idx >= tokens.len() {
        return Err(CommandError::InvalidArgument(
            parse_errors::MISSING_ARGUMENT,
        ));
    }
    let b = &tokens[*idx];
    let s = ::core::str::from_utf8(b.as_ref())
        .map_err(|_| CommandError::InvalidArgument(parse_errors::INVALID_UTF8))?;
    let n = s
        .parse::<isize>()
        .map_err(|_| CommandError::InvalidArgument(parse_errors::INVALID_INTEGER))?;
    *idx += 1;
    Ok(n)
}

#[cfg(test)]
mod derived_names {
    use crate::command::{EchoParsed, GetParsed, PingParsed};

    use super::CommandSyntax;

    #[test]
    fn derived_command_constants() {
        assert_eq!(PingParsed::COMMAND_NAME, "PING");
        assert_eq!(EchoParsed::COMMAND_NAME, "ECHO");
        assert_eq!(GetParsed::COMMAND_NAME, "GET");
    }
}
