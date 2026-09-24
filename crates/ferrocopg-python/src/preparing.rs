//! Native preparation and execution ownership for the boundary prototype.
//! Not routed into public queries until the complete native operation is ready.

use num_bigint::{BigInt, BigUint, Sign};
use pyo3::exceptions::{PyIndexError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

const NO: u8 = 1;
const YES: u8 = 2;
const SHOULD: u8 = 3;

#[derive(Clone)]
enum Counter {
    Small(u64),
    Large(BigUint),
}

impl Default for Counter {
    fn default() -> Self {
        Self::Small(0)
    }
}

impl Counter {
    fn increment(&mut self) {
        match self {
            Self::Small(value) => {
                *self = match value.checked_add(1) {
                    Some(next) => Self::Small(next),
                    None => Self::Large(BigUint::from(*value) + 1u8),
                };
            }
            Self::Large(value) => *value += 1u8,
        }
    }

    fn as_biguint(&self) -> BigUint {
        match self {
            Self::Small(value) => BigUint::from(*value),
            Self::Large(value) => value.clone(),
        }
    }
}

impl fmt::Display for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Small(value) => value.fmt(f),
            Self::Large(value) => value.fmt(f),
        }
    }
}

#[derive(Clone)]
struct IntegerSetting {
    value: BigInt,
    small: Option<u64>,
}

impl IntegerSetting {
    fn new(value: BigInt) -> Self {
        let small = u64::try_from(&value).ok();
        Self { value, small }
    }

    fn reached(&self, count: &Counter) -> bool {
        if self.value.sign() == Sign::Minus {
            return true;
        }
        match count {
            Counter::Small(count) => self.small.is_some_and(|limit| *count >= limit),
            Counter::Large(count) => count >= self.value.magnitude(),
        }
    }

    fn exceeded_by(&self, size: usize) -> bool {
        self.value.sign() == Sign::Minus
            || self
                .small
                .is_some_and(|limit| size as u128 > u128::from(limit))
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    query: Vec<u8>,
    types: Vec<u32>,
}

#[derive(Clone, Default)]
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
        if self.clock == u128::MAX {
            // Recency is relative: compact without changing observable order.
            let old_order = std::mem::take(&mut self.order);
            for (index, key) in old_order.into_values().enumerate() {
                let position = index as u128 + 1;
                self.values.get_mut(&key).expect("ordered key exists").1 = position;
                self.order.insert(position, key);
            }
            self.clock = self.order.len() as u128;
        }
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

#[derive(Clone)]
pub(crate) struct PreparationState {
    prepare_threshold: Option<IntegerSetting>,
    prepared_max: IntegerSetting,
    counts: Ordered<Counter>,
    names: Ordered<Vec<u8>>,
    next_name: Counter,
    to_flush: VecDeque<Option<Vec<u8>>>,
}

pub(crate) enum ExecutionError {
    Backend(ferrocopg_postgres::ProbeError),
    EmptyCache,
}

impl From<ferrocopg_postgres::ProbeError> for ExecutionError {
    fn from(error: ferrocopg_postgres::ProbeError) -> Self {
        Self::Backend(error)
    }
}

pub(crate) struct ExecutionState {
    preparation: PreparationState,
    statements: HashMap<Vec<u8>, u64>,
    configuration: Option<Arc<ExecutionConfiguration>>,
}

pub(crate) struct ExecutionConfiguration {
    threshold: Option<BigInt>,
    maximum: BigInt,
}

pub(crate) type SharedExecutionConfiguration = Mutex<Arc<ExecutionConfiguration>>;

impl ExecutionConfiguration {
    pub(crate) fn new(threshold: Option<BigInt>, maximum: BigInt) -> Self {
        Self { threshold, maximum }
    }
}

impl ExecutionState {
    pub(crate) fn new() -> Self {
        Self {
            preparation: PreparationState::new(),
            statements: HashMap::new(),
            configuration: None,
        }
    }

    pub(crate) fn sync_configuration(
        &mut self,
        source: &SharedExecutionConfiguration,
    ) -> Result<(), ferrocopg_postgres::ProbeError> {
        let configuration = Arc::clone(&*source.lock().map_err(|_| {
            ferrocopg_postgres::ProbeError::BadParam(
                "execution configuration mutex is poisoned".to_owned(),
            )
        })?);
        if self
            .configuration
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &configuration))
        {
            return Ok(());
        }
        self.preparation.prepare_threshold =
            configuration.threshold.clone().map(IntegerSetting::new);
        self.preparation.prepared_max = IntegerSetting::new(configuration.maximum.clone());
        self.configuration = Some(configuration);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> NativePreparationState {
        NativePreparationState {
            state: self.preparation.clone(),
        }
    }

    pub(crate) fn forget(&mut self) {
        self.preparation.clear();
        self.preparation.to_flush.clear();
        self.statements.clear();
    }

    pub(crate) fn clear(&mut self, session: &mut ferrocopg_postgres::SyncNoTlsSession) {
        self.preparation.clear();
        self.maintain(session);
    }

    fn maintain(&mut self, session: &mut ferrocopg_postgres::SyncNoTlsSession) {
        while let Some(name) = self.preparation.to_flush.pop_front() {
            if let Some(name) = name {
                if let Some(id) = self.statements.remove(&name) {
                    let _ = session.close_prepared(id);
                }
            } else {
                for (_, id) in self.statements.drain() {
                    let _ = session.close_prepared(id);
                }
            }
        }
    }

    pub(crate) fn execute(
        &mut self,
        session: &mut ferrocopg_postgres::SyncNoTlsSession,
        configuration: &SharedExecutionConfiguration,
        query: &str,
        params: &[ferrocopg_postgres::BoundParam],
        prepare: Option<bool>,
        format: ferrocopg_postgres::WireFormat,
    ) -> Result<ferrocopg_postgres::ResultSet, ExecutionError> {
        self.sync_configuration(configuration)?;
        let key = Key {
            query: query.as_bytes().to_vec(),
            types: params.iter().map(|param| param.oid).collect(),
        };
        let (decision, name) = self.preparation.get(&key, prepare);
        let id = match decision {
            SHOULD => {
                let statement = session.prepare_params(query, &key.types)?;
                self.statements.insert(name.clone(), statement.statement_id);
                Some(statement.statement_id)
            }
            YES => Some(*self.statements.get(&name).ok_or_else(|| {
                ferrocopg_postgres::ProbeError::BadParam(
                    "native prepared statement has no execution owner".to_owned(),
                )
            })?),
            _ => None,
        };
        let result = match id {
            Some(id) => session.run_prepared_params_format(id, params, format),
            None => session.run_params_format(query, params, format),
        };
        // A signal handler may have changed policy during I/O, before caching.
        self.sync_configuration(configuration)?;
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if decision == SHOULD {
                    if let Some(id) = self.statements.remove(&name) {
                        let _ = session.close_prepared(id);
                    }
                }
                return Err(error.into());
            }
        };
        if self.preparation.add(key.clone(), decision, name.clone()) {
            let status = if result.command_tag.as_deref() == Some("") {
                0
            } else if result.is_tuples {
                2
            } else {
                1
            };
            self.preparation
                .validate(
                    &key,
                    decision,
                    &[(
                        status,
                        result
                            .command_tag
                            .as_ref()
                            .map(|tag| tag.as_bytes().to_vec()),
                    )],
                )
                .ok_or(ExecutionError::EmptyCache)?;
        }
        // Invalid/empty results must not leave an untracked server statement.
        if decision == SHOULD && self.preparation.names.get(&key).is_none() {
            if let Some(id) = self.statements.remove(&name) {
                let _ = session.close_prepared(id);
            }
        }
        self.maintain(session);
        Ok(result)
    }
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
            prepare_threshold: Some(IntegerSetting::new(BigInt::from(5))),
            prepared_max: IntegerSetting::new(BigInt::from(100)),
            counts: Ordered::default(),
            names: Ordered::default(),
            next_name: Counter::default(),
            to_flush: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &Key, prepare: Option<bool>) -> (u8, Vec<u8>) {
        let Some(threshold) = self.prepare_threshold.as_ref() else {
            return (NO, Vec::new());
        };
        if prepare == Some(false) {
            return (NO, Vec::new());
        }
        if let Some(name) = self.names.get(key).filter(|name| !name.is_empty()) {
            return (YES, name.clone());
        }
        let count = self.counts.get(key).unwrap_or(&Counter::Small(0));
        if threshold.reached(count) || prepare == Some(true) {
            let name = format!("_pg3_{}", self.next_name);
            self.next_name.increment();
            (SHOULD, name.into_bytes())
        } else {
            (NO, Vec::new())
        }
    }

    fn add(&mut self, key: Key, prep: u8, name: Vec<u8>) -> bool {
        if self.prepare_threshold.is_none() {
            return false;
        }
        if let Some(mut count) = self.counts.remove(&key) {
            if prep == SHOULD {
                self.names.insert_last(key, name);
            } else {
                count.increment();
                self.counts.insert_last(key, count);
            }
            false
        } else if let Some(name) = self.names.remove(&key) {
            self.names.insert_last(key, name);
            false
        } else {
            if prep == SHOULD {
                self.names.insert_last(key, name);
            } else {
                self.counts.insert_last(key, Counter::Small(1));
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
            if self.prepared_max.exceeded_by(self.counts.values.len()) {
                self.counts.pop_first()?;
            }
            if self.prepared_max.exceeded_by(self.names.values.len()) {
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
pub(crate) struct NativePreparationState {
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
    fn prepare_threshold(&self) -> Option<BigInt> {
        self.state
            .prepare_threshold
            .as_ref()
            .map(|setting| setting.value.clone())
    }

    #[setter]
    fn set_prepare_threshold(&mut self, value: Option<BigInt>) {
        self.state.prepare_threshold = value.map(IntegerSetting::new);
    }

    #[getter]
    fn prepared_max(&self) -> BigInt {
        self.state.prepared_max.value.clone()
    }

    #[setter]
    fn set_prepared_max(&mut self, value: BigInt) {
        self.state.prepared_max = IntegerSetting::new(value);
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
                .map(|(key, count)| {
                    (
                        PyBytes::new(py, &key.query),
                        key.types.clone(),
                        count.as_biguint(),
                    )
                })
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
        result.set_item("next_name", self.state.next_name.as_biguint())?;
        result.set_item(
            "prepare_threshold",
            self.state
                .prepare_threshold
                .as_ref()
                .map(|setting| &setting.value),
        )?;
        result.set_item("prepared_max", &self.state.prepared_max.value)?;
        Ok(result)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativePreparationState>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(query: &[u8]) -> Key {
        Key {
            query: query.to_vec(),
            types: vec![21],
        }
    }

    #[test]
    fn counts_promote_without_changing_threshold_decisions() {
        let mut state = PreparationState::new();
        let key = key(b"select $1");
        let limit = BigInt::from(u64::MAX) + 2u8;
        state.prepare_threshold = Some(IntegerSetting::new(limit.clone()));
        state
            .counts
            .insert_last(key.clone(), Counter::Small(u64::MAX));
        assert_eq!(state.get(&key, None).0, NO);
        assert!(!state.add(key.clone(), NO, Vec::new()));
        assert_eq!(state.get(&key, None).0, NO);
        assert!(!state.add(key.clone(), NO, Vec::new()));
        assert_eq!(state.get(&key, None).0, SHOULD);
        assert_eq!(
            state.counts.get(&key).unwrap().as_biguint(),
            *limit.magnitude()
        );
    }

    #[test]
    fn statement_names_remain_unique_across_machine_word_limit() {
        let mut state = PreparationState::new();
        state.next_name = Counter::Small(u64::MAX);
        let key = key(b"select $1");
        assert_eq!(state.get(&key, Some(true)).1, b"_pg3_18446744073709551615");
        assert_eq!(state.get(&key, Some(true)).1, b"_pg3_18446744073709551616");
        assert_eq!(state.get(&key, Some(true)).1, b"_pg3_18446744073709551617");
    }

    #[test]
    fn recency_compaction_preserves_touch_and_eviction_order() {
        let mut values = Ordered::default();
        values.insert_last(key(b"a"), 1);
        values.insert_last(key(b"b"), 2);
        values.insert_last(key(b"c"), 3);
        values.clock = u128::MAX;
        values.insert_last(key(b"b"), 4);
        assert_eq!(values.clock, 3);
        assert_eq!(values.pop_first(), Some(1));
        assert_eq!(values.pop_first(), Some(3));
        assert_eq!(values.pop_first(), Some(4));
        assert!(values.values.is_empty());
        assert!(values.order.is_empty());
    }

    #[test]
    fn integer_settings_compare_without_narrowing() {
        let huge = BigInt::from(1u8) << 20000usize;
        let positive = IntegerSetting::new(huge.clone());
        let negative = IntegerSetting::new(-huge);
        assert!(!positive.reached(&Counter::Small(u64::MAX)));
        assert!(!positive.exceeded_by(usize::MAX));
        assert!(negative.reached(&Counter::Small(0)));
        assert!(negative.exceeded_by(0));
        assert!(positive.reached(&Counter::Large(positive.value.magnitude().clone())));
    }
}
