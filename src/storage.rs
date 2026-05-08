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

pub struct PushOperation {
    key: Bytes,
    values: Vec<Bytes>,
}

pub fn set(operation: SetOperation) {
    STORAGE.lock().unwrap().insert(
        operation.key,
        StoredValue {
            value: Value::Single(operation.value),
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
                let ret = match stored_value.value {
                    Value::Single(ref value) => Some(Bytes::clone(value)),
                    Value::List(ref values) => values.last().cloned(),
                };

                // Ask the borrow checker to help us to returning a value that would hold the lock
                drop(lock);

                ret
            }
        }
        None => None,
    }
}

pub fn push(operation: PushOperation) -> usize {
    let mut lock = STORAGE.lock().unwrap();
    let entry = lock.entry(operation.key).or_insert_with(|| StoredValue {
        value: Value::List(vec![]),
        expires_at: None,
    });
    let stub = Value::List(vec![]);
    let mut new_value = match std::mem::replace(&mut entry.value, stub) {
        Value::List(values) => values,
        Value::Single(value) => vec![value],
    };
    new_value.extend(operation.values);
    let ret = new_value.len();

    entry.value = Value::List(new_value);
    ret
}

impl SetOperation {
    pub fn try_from_args(args: &[RedisValue]) -> Result<Self, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::InvalidArgument(
                "SET command requires at least two arguments",
            ));
        }

        let RedisValue::BulkString(key) = &args[0] else {
            return Err(CommandError::InvalidArgument(
                "SET command requires a string argument",
            ));
        };
        let key = key.clone();

        let RedisValue::BulkString(value) = &args[1] else {
            return Err(CommandError::InvalidArgument(
                "SET command requires a string argument",
            ));
        };
        let value = value.clone();

        let expiration_ms = (args.len() >= 3)
            .then(|| -> Result<i64, CommandError> {
                let RedisValue::BulkString(opt) = &args[2] else {
                    return Err(CommandError::InvalidArgument(
                        "SET command requires a string argument",
                    ));
                };
                let mult = match opt.as_ref() {
                    b"EX" => 1000,
                    b"PX" => 1,
                    _ => {
                        return Err(CommandError::InvalidArgument(
                            "Unexpected argument after key",
                        ));
                    }
                };

                let RedisValue::BulkString(exp) = args.get(3).ok_or(
                    CommandError::InvalidArgument("Missing value after expiration type"),
                )?
                else {
                    return Err(CommandError::InvalidArgument(
                        "SET command requires a string argument",
                    ));
                };

                str::from_utf8(exp.as_ref())
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

impl PushOperation {
    pub fn try_from_args(args: &[RedisValue]) -> Result<Self, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::InvalidArgument(
                "PUSH command requires at least two arguments",
            ));
        }

        let RedisValue::BulkString(key) = &args[0] else {
            return Err(CommandError::InvalidArgument(
                "PUSH command requires a string key",
            ));
        };

        let mut values = Vec::with_capacity(args.len() - 1);
        for arg in &args[1..] {
            let RedisValue::BulkString(v) = arg else {
                return Err(CommandError::InvalidArgument(
                    "PUSH command requires string arguments",
                ));
            };
            values.push(v.clone());
        }

        Ok(Self {
            key: key.clone(),
            values,
        })
    }
}
struct StoredValue {
    value: Value,
    expires_at: Option<DateTime<Utc>>,
}

enum Value {
    Single(Bytes),
    List(Vec<Bytes>),
}
