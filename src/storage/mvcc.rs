use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
    u64,
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::engine::Engine;

pub type Version = u64;

// possibly mutithread call, need to add Arc<Mutex> to ensure safe
// •	Arc 允许多个线程共享对同一数据的所有权。
// •	Mutex 确保在任意时刻，只有一个线程可以访问或修改共享数据。
// •	结合使用 Arc<Mutex<T>>，你可以在多线程环境下安全地共享和修改数据
pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    // start transaction(MvccTransaction)
    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

pub struct TransactionState {
    // current version
    pub version: Version,
    // current active versions
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false;
        } else {
            return version <= self.version;
        }
    }
}

// NextVersion 0
// TxnActive 1-100 1-101 1-102...
// Version key1-101 key2-101...
// scan preifix

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version, Vec<u8>),
    Version(Vec<u8>, Version),
}

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

impl<E: Engine> MvccTransaction<E> {
    // start a transction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // get the current transaction number
        let mut engine = eng.lock()?;
        let new_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1, // the first trasaction
        };
        // store next version
        engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(new_version + 1))?,
        )?;
        // get active transaction list
        let active_versions = Self::scan_active(&mut engine)?;
        // set current to active, note that current active list(get before) doesn't contain current version
        engine.set(MvccKey::TxnActive(new_version).encode(), vec![])?;
        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: new_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    // •	self.engine 是一个 Mutex 类型的变量，这意味着它包含一个被锁保护的资源。
    // •	self.engine.lock() 获取这个锁。通过 ? 操作符，若获取锁失败，会将错误向上返回。
    // •	成功获取锁后，eng 是 MutexGuard 类型，拥有对 self.engine 内部数据的可变访问权限。由于 MutexGuard 会在作用域结束时自动释放锁，eng 仅在当前代码块内有效。
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
    }

    // modify/delete data
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        let mut engine = self.engine.lock()?;
        // check conflict
        // eg: active list: 3 4 5
        // current version: 6
        // key1-3 key2-4 key3-5
        // scan from 3 -- max version
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1), // if no active, start from current version + 1
        )
        .encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();
        // only need to check last value
        // eg: active list: 3 4 5 
        // current version: 6
        // 1. key is sorted, ascending sequence
        // 2. if version 10 modify data and commit, 6 is conflict to modify same data
        // 3. if active version has modified the data, like 4, version 5 cannot modify this key
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // check if this version is visible
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "UNexpected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        // 记录这个 version 写入了哪些 key，用于回滚事务
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode(),
            vec![],
        )?;

        // 写入实际的 key value 数据
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode(),
            bincode::serialize(&value)?,
        )?;
        Ok(())
    }

    // check data start by table name as prefix
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "UNexpected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(active_versions)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
