//! Generic static messages for parsers (no Redis command wording).

pub const WRONG_ARGUMENT_COUNT: &str = "wrong number of arguments";
pub const MISSING_ARGUMENT: &str = "missing required argument";
pub const EXTRA_TRAILING: &str = "unexpected trailing arguments";
pub const INVALID_OPTION_TOKEN: &str = "unrecognized option";
pub const MISSING_OPTION_ARGUMENT: &str = "missing value after option keyword";
pub const INVALID_UTF8: &str = "argument is not valid UTF-8";
pub const INVALID_INTEGER: &str = "argument is not a valid signed integer";
pub const INVALID_NUMBER: &str = "argument is not a valid number";

pub const RESPONSE_NOT_STRING_ELEMENTS: &str = "decoded array entries must be strings";
pub const TOP_LEVEL_SHAPE: &str = "top-level input must be an array tokenization or plain line";
pub const UNKNOWN_COMMAND_WORD: &str = "unknown command";

pub const VALUE_TYPE_MISMATCH: &str = "stored value does not match expected type";
