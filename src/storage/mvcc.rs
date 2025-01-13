use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
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
        Ok(MvccTransaction::begin(self.engine.clone()))
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

// NextVersion 0
// TxnActive 1-100
//           1-101
//           1-102
// scan preifix

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
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
        // add to active trasaction list
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
        let mut eng = self.engine.lock()?;
        eng.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        eng.get(key)
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
