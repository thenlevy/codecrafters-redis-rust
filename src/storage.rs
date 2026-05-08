use super::CommandError;

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

pub struct RangeOperation {
    key: Bytes,
    start: isize,
    end: isize,
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

/// Redis-style inclusive range on a list with length `len`. Returns `(start_idx, stop_idx)`
/// inclusive, or [`None`] when the logical range is empty.
fn list_range_bounds(start: isize, end: isize, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let l = len as isize;

    let mut s = start;
    if s < 0 {
        s += l;
        if s < 0 {
            s = 0;
        }
    }
    if s >= l {
        return None;
    }

    let mut e = end;
    if e < 0 {
        e += l;
        if e < 0 {
            return None;
        }
    }
    if e >= l {
        e = l - 1;
    }

    if s > e {
        None
    } else {
        Some((s as usize, e as usize))
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
    pub fn try_from_args(args: &[Bytes]) -> Result<Self, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::InvalidArgument(
                "SET command requires at least two arguments",
            ));
        }

        let key = args[0].clone();
        let value = args[1].clone();

        let expiration_ms = (args.len() >= 3)
            .then(|| -> Result<i64, CommandError> {
                let mult = match args[2].as_ref() {
                    b"EX" => 1000,
                    b"PX" => 1,
                    _ => {
                        return Err(CommandError::InvalidArgument(
                            "Unexpected argument after key",
                        ));
                    }
                };

                let exp = args.get(3).ok_or(CommandError::InvalidArgument(
                    "Missing value after expiration type",
                ))?;

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
    pub fn try_from_args(args: &[Bytes]) -> Result<Self, CommandError> {
        if args.len() < 2 {
            return Err(CommandError::InvalidArgument(
                "RPUSH command requires at least two arguments",
            ));
        }

        let key = args[0].clone();
        let values = args[1..].to_vec();

        Ok(Self { key, values })
    }
}

impl RangeOperation {
    pub fn try_from_args(args: &[Bytes]) -> Result<Self, CommandError> {
        if args.len() != 3 {
            return Err(CommandError::InvalidArgument(
                "LRANGE command requires key, start, and stop",
            ));
        }

        let key = args[0].clone();

        let start = str::from_utf8(args[1].as_ref())
            .map_err(|_| {
                CommandError::InvalidArgument("LRANGE start must be a valid UTF-8 integer")
            })?
            .parse::<isize>()
            .map_err(|_| CommandError::InvalidArgument("LRANGE start must be an integer"))?;

        let end = str::from_utf8(args[2].as_ref())
            .map_err(|_| {
                CommandError::InvalidArgument("LRANGE stop must be a valid UTF-8 integer")
            })?
            .parse::<isize>()
            .map_err(|_| CommandError::InvalidArgument("LRANGE stop must be an integer"))?;

        Ok(Self { key, start, end })
    }
}

pub fn get_range(operation: RangeOperation) -> Result<Vec<Bytes>, CommandError> {
    let RangeOperation { key, start, end } = operation;

    let mut lock = STORAGE.lock().unwrap();
    match lock.get(&key) {
        None => Ok(vec![]),
        Some(stored_value) => {
            if stored_value.expires_at.is_some_and(|d| d < Utc::now()) {
                lock.remove(&key);
                Ok(vec![])
            } else {
                let list_slice = match &stored_value.value {
                    Value::List(values) => {
                        let bounds = list_range_bounds(start, end, values.len());
                        let out = bounds
                            .map(|(s, e)| values[s..=e].to_vec())
                            .unwrap_or_default();
                        Ok(out)
                    }
                    Value::Single(_) => Err(CommandError::InvalidArgument("value is not a list")),
                };

                drop(lock);
                list_slice
            }
        }
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
