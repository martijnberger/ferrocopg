//! Preparation transitions for the execution-boundary prototype.
//! Not routed into public queries until the complete native operation is ready.

use pyo3::exceptions::{PyIndexError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

const NO: u8 = 1;
const YES: u8 = 2;
const SHOULD: u8 = 3;

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    query: Vec<u8>,
    types: Vec<u32>,
}

#[derive(Default)]
struct Ordered<T> {
    values: HashMap<Arc<Key>, (T, u128)>,
    order: BTreeMap<u128, Arc<Key>>,
    clock: u128,
}

impl<T> Ordered<T> {
    fn get(&self, key: &Key) -> Option<&T> {
        self.values.get(key).map(|entry| &entry.0)
    }

    fn remove(&mut self, key: &Key) -> Option<T> {
        let (value, position) = self.values.remove(key)?;
        self.order.remove(&position);
        Some(value)
    }

    fn insert_last(&mut self, key: Key, value: T) {
        self.remove(&key);
        let key = Arc::new(key);
        self.clock += 1;
        self.order.insert(self.clock, Arc::clone(&key));
        self.values.insert(key, (value, self.clock));
    }

    fn pop_first(&mut self) -> Option<T> {
        let (_, key) = self.order.first_key_value()?;
        let key = Arc::clone(key);
        self.remove(&key)
    }

    fn clear(&mut self) {
        self.values.clear();
        self.order.clear();
        self.clock = 0;
    }

    fn entries(&self) -> impl Iterator<Item = (&Key, &T)> {
        self.order.values().map(|key| {
            (
                key.as_ref(),
                &self.values.get(key).expect("ordered key exists").0,
            )
        })
    }
}

pub(crate) struct PreparationState {
    prepare_threshold: Option<i64>,
    prepared_max: i64,
    counts: Ordered<u64>,
    names: Ordered<Vec<u8>>,
    next_name: u64,
    to_flush: VecDeque<Option<Vec<u8>>>,
}

fn check_preparation(prep: u8) -> PyResult<()> {
    if matches!(prep, NO | YES | SHOULD) {
        Ok(())
    } else {
        Err(PyValueError::new_err("unknown preparation decision"))
    }
}

fn invalidates(command: &[u8]) -> bool {
    [b"DROP".as_slice(), b"ALTER", b"ROLLBACK", b"DISCARD"]
        .iter()
        .any(|prefix| {
            command.starts_with(prefix)
                && command
                    .get(prefix.len())
                    .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        })
}

impl PreparationState {
    fn new() -> Self {
        Self {
            prepare_threshold: Some(5),
            prepared_max: 100,
            counts: Ordered::default(),
            names: Ordered::default(),
            next_name: 0,
            to_flush: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &Key, prepare: Option<bool>) -> (u8, Vec<u8>) {
        let Some(threshold) = self.prepare_threshold else {
            return (NO, Vec::new());
        };
        if prepare == Some(false) {
            return (NO, Vec::new());
        }
        if let Some(name) = self.names.get(key).filter(|name| !name.is_empty()) {
            return (YES, name.clone());
        }
        let count = self.counts.get(key).copied().unwrap_or(0);
        if i128::from(count) >= i128::from(threshold) || prepare == Some(true) {
            let name = format!("_pg3_{}", self.next_name);
            self.next_name += 1;
            (SHOULD, name.into_bytes())
        } else {
            (NO, Vec::new())
        }
    }

    fn add(&mut self, key: Key, prep: u8, name: Vec<u8>) -> bool {
        if self.prepare_threshold.is_none() {
            return false;
        }
        if let Some(count) = self.counts.remove(&key) {
            if prep == SHOULD {
                self.names.insert_last(key, name);
            } else {
                self.counts.insert_last(key, count + 1);
            }
            false
        } else if let Some(name) = self.names.remove(&key) {
            self.names.insert_last(key, name);
            false
        } else {
            if prep == SHOULD {
                self.names.insert_last(key, name);
            } else {
                self.counts.insert_last(key, 1);
            }
            true
        }
    }

    fn validate(&mut self, key: &Key, prep: u8, results: &[(u32, Option<Vec<u8>>)]) -> Option<()> {
        if (!self.names.values.is_empty() || prep == SHOULD)
            && results.iter().any(|(status, command)| {
                *status == 1 && command.as_ref().is_some_and(|value| invalidates(value))
            })
            && self.clear()
        {
            return Some(());
        }
        if results.len() != 1 || !matches!(results[0].0, 1 | 2) {
            self.discard(key);
        } else {
            if self.counts.values.len() as i128 > i128::from(self.prepared_max) {
                self.counts.pop_first()?;
            }
            if self.names.values.len() as i128 > i128::from(self.prepared_max) {
                self.to_flush.push_back(Some(self.names.pop_first()?));
            }
        }
        Some(())
    }

    fn discard(&mut self, key: &Key) {
        self.names.remove(key);
        self.counts.remove(key);
    }

    fn clear(&mut self) -> bool {
        self.counts.clear();
        if self.names.values.is_empty() {
            false
        } else {
            self.names.clear();
            self.to_flush.clear();
            self.to_flush.push_back(None);
            true
        }
    }
}

// Only this adapter allocates Python inspection values. The executor will use
// PreparationState directly, without another Python cache or callback owner.
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct NativePreparationState {
    state: PreparationState,
}

#[pymethods]
impl NativePreparationState {
    #[new]
    fn new() -> Self {
        Self {
            state: PreparationState::new(),
        }
    }

    #[getter]
    fn prepare_threshold(&self) -> Option<i64> {
        self.state.prepare_threshold
    }

    #[setter]
    fn set_prepare_threshold(&mut self, value: Option<i64>) {
        self.state.prepare_threshold = value;
    }

    #[getter]
    fn prepared_max(&self) -> i64 {
        self.state.prepared_max
    }

    #[setter]
    fn set_prepared_max(&mut self, value: i64) {
        self.state.prepared_max = value;
    }

    #[pyo3(signature = (query, types, prepare=None))]
    fn get(
        &mut self,
        py: Python<'_>,
        query: Vec<u8>,
        types: Vec<u32>,
        prepare: Option<bool>,
    ) -> (u8, Py<PyBytes>) {
        let (prep, name) = self.state.get(&Key { query, types }, prepare);
        (prep, PyBytes::new(py, &name).unbind())
    }

    fn add(&mut self, query: Vec<u8>, types: Vec<u32>, prep: u8, name: Vec<u8>) -> PyResult<bool> {
        check_preparation(prep)?;
        Ok(self.state.add(Key { query, types }, prep, name))
    }

    fn validate(
        &mut self,
        query: Vec<u8>,
        types: Vec<u32>,
        prep: u8,
        results: Vec<(u32, Option<Vec<u8>>)>,
    ) -> PyResult<()> {
        check_preparation(prep)?;
        self.state
            .validate(&Key { query, types }, prep, &results)
            .ok_or_else(|| PyKeyError::new_err("dictionary is empty"))
    }

    fn discard(&mut self, query: Vec<u8>, types: Vec<u32>) {
        self.state.discard(&Key { query, types });
    }

    fn clear(&mut self) -> bool {
        self.state.clear()
    }

    fn pop_flush(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        self.state
            .to_flush
            .pop_front()
            .map(|name| name.map(|name| PyBytes::new(py, &name).unbind()))
            .ok_or_else(|| PyIndexError::new_err("pop from an empty deque"))
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let result = PyDict::new(py);
        result.set_item(
            "counts",
            self.state
                .counts
                .entries()
                .map(|(key, count)| (PyBytes::new(py, &key.query), key.types.clone(), *count))
                .collect::<Vec<_>>(),
        )?;
        result.set_item(
            "names",
            self.state
                .names
                .entries()
                .map(|(key, name)| {
                    (
                        PyBytes::new(py, &key.query),
                        key.types.clone(),
                        PyBytes::new(py, name),
                    )
                })
                .collect::<Vec<_>>(),
        )?;
        result.set_item(
            "to_flush",
            self.state
                .to_flush
                .iter()
                .map(|name| name.as_ref().map(|name| PyBytes::new(py, name)))
                .collect::<Vec<_>>(),
        )?;
        result.set_item("next_name", self.state.next_name)?;
        result.set_item("prepare_threshold", self.state.prepare_threshold)?;
        result.set_item("prepared_max", self.state.prepared_max)?;
        Ok(result)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativePreparationState>()
}
