use super::{CommandError, RedisValue};

use {
    bytes::Bytes,
    chrono::{DateTime, Duration, Utc},
    std::{
        collections::HashMap,
        sync::{Arc, LazyLock, Mutex},
    },
};

static STORAGE: LazyLock<Arc<Mutex<HashMap<Bytes, StoredValue>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

pub struct SetOperation {
    key: Bytes,
    value: Bytes,
    expiration: Option<DateTime<Utc>>,
}

pub fn set(operation: SetOperation) {
    STORAGE.lock().unwrap().insert(
        operation.key,
        StoredValue {
            value: operation.value,
            expires_at: operation.expiration,
        },
    );
}

pub fn get(key: Bytes) -> Option<Bytes> {
    let mut lock = STORAGE.lock().unwrap();
    match lock.get(&key) {
        Some(stored_value) => {
            if stored_value.expires_at.is_some_and(|d| d < Utc::now()) {
                lock.remove(&key);
                None
            } else {
                let ret = Bytes::clone(&stored_value.value);

                // Ask the borrow checker to help us to returning a value that would hold the lock
                drop(lock);

                Some(ret)
            }
        }
        None => None,
    }
}

impl SetOperation {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            expiration: None,
        }
    }

    pub fn expires_in_seconds(self, seconds: i64) -> Self {
        Self {
            expiration: Some(Utc::now() + Duration::seconds(seconds)),
            ..self
        }
    }

    pub fn expires_in_milliseconds(self, milliseconds: i64) -> Self {
        Self {
            expiration: Some(Utc::now() + Duration::milliseconds(milliseconds)),
            ..self
        }
    }

    pub fn try_from_args(args: &[RedisValue]) -> Result<Self, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::InvalidArgument(
                "SET command requires at least two arguments",
            ));
        }

        let Some(key) = args[0].try_bytes() else {
            return Err(CommandError::InvalidArgument(
                "SET command requires a string argument",
            ));
        };

        let Some(value) = args[1].try_bytes() else {
            return Err(CommandError::InvalidArgument(
                "SET command requires a string argument",
            ));
        };

        let expiration_ms =
            (args.len() >= 3)
                .then(|| -> Result<i64, CommandError> {
                    let mult = match args[2].try_bytes().as_deref().ok_or(
                        CommandError::InvalidArgument("SET command requires a string argument"),
                    )? {
                        b"EX" => 1000,
                        b"PX" => 1,
                        _ => {
                            return Err(CommandError::InvalidArgument(
                                "Unexpected argument after key",
                            ));
                        }
                    };

                    let value = args
                        .get(3)
                        .ok_or(CommandError::InvalidArgument(
                            "Missing value after expiration type",
                        ))?
                        .try_bytes()
                        .ok_or(CommandError::InvalidArgument(
                            "SET command requires a string argument",
                        ))?;

                    str::from_utf8(value.as_ref())
                        .map_err(|_| CommandError::InvalidArgument("Invalid expiration value"))?
                        .parse::<i64>()
                        .map_err(|_| CommandError::InvalidArgument("Invalid expiration value"))
                        .map(|ms| ms * mult)
                })
                .transpose()?;

        Ok(Self {
            key,
            value,
            expiration: expiration_ms.map(|ms| Utc::now() + Duration::milliseconds(ms)),
        })
    }
}

struct StoredValue {
    value: Bytes,
    expires_at: Option<DateTime<Utc>>,
}
