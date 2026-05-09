use crate::command::{CommandError, parse_errors};

use {
    bytes::Bytes,
    chrono::{DateTime, Utc},
    std::{
        collections::{HashMap, VecDeque},
        sync::{Arc, LazyLock, Mutex},
    },
};

static STORAGE: LazyLock<Arc<Mutex<HashMap<Bytes, StoredValue>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

pub struct SetOperation {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) expiration: Option<DateTime<Utc>>,
}

pub enum PushKind {
    RPush,
    LPush,
}

pub struct PushOperation {
    pub(crate) kind: PushKind,
    pub(crate) key: Bytes,
    pub(crate) values: Vec<Bytes>,
}

pub struct RangeOperation {
    pub(crate) key: Bytes,
    pub(crate) start: isize,
    pub(crate) end: isize,
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
                    Value::List(ref values) => values.back().cloned(),
                };

                // Ask the borrow checker to help us to returning a value that would hold the lock
                drop(lock);

                ret
            }
        }
        None => None,
    }
}

/// Inclusive `(start, stop)` slice bounds for a list of length `len`, matching Redis
/// [`LRANGE`](https://redis.io/docs/latest/commands/lrange/) index rules:
/// zero-based inclusive range, negative indices count from the end (`-1` is last),
/// `start` is clamped to `0` if it is still negative after one `+ len` adjustment,
/// `stop` beyond the tail is clamped to `len - 1`, and an empty range is produced when
/// `start` is past the tail, or `start > stop` after normalization (including when
/// `stop` remains negative after one `+ len` adjustment—see “Out-of-range indexes” on
/// that page). Returns [`None`] for an empty logical range.
fn list_range_bounds(mut start: isize, mut end: isize, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as isize;

    if start < 0 {
        start += len;
        if start < 0 {
            start = 0;
        }
    }
    if start >= len {
        return None;
    }

    if end < 0 {
        end += len;
        if end < 0 {
            return None;
        }
    }
    if end >= len {
        end = len - 1;
    }

    if start > end {
        None
    } else {
        Some((start as usize, end as usize))
    }
}

pub fn push(operation: PushOperation) -> usize {
    let mut lock = STORAGE.lock().unwrap();
    let entry = lock.entry(operation.key).or_insert_with(|| StoredValue {
        value: Value::List(VecDeque::new()),
        expires_at: None,
    });
    let stub = Value::List(VecDeque::new());
    let mut new_value = match std::mem::replace(&mut entry.value, stub) {
        Value::List(values) => values,
        Value::Single(value) => VecDeque::from_iter([value]),
    };
    match operation.kind {
        PushKind::RPush => new_value.extend(operation.values),
        PushKind::LPush => {
            for v in operation.values.into_iter().rev() {
                new_value.push_front(v);
            }
        }
    }
    let ret = new_value.len();

    entry.value = Value::List(new_value);
    ret
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
                            .map(|(s, e)| {
                                values
                                    .iter()
                                    .skip(s)
                                    .take(e - s + 1)
                                    .map(|v| Bytes::clone(v))
                                    .collect()
                            })
                            .unwrap_or_default();
                        Ok(out)
                    }
                    Value::Single(_) => Err(CommandError::InvalidArgument(
                        parse_errors::VALUE_TYPE_MISMATCH,
                    )),
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
    List(VecDeque<Bytes>),
}
