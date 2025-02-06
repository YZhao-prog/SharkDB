use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    u64,
};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{
    engine::Engine,
    keycode::{deserialize_key, serialize_key},
};

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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}


impl MvccKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

impl<E: Engine> MvccTransaction<E> {
    // start a transction
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // get the current transaction number
        let mut engine = eng.lock()?;
        let new_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1, // the first trasaction
        };
        // store next version
        engine.set(
            MvccKey::NextVersion.encode()?,
            bincode::serialize(&(new_version + 1))?,
        )?;
        // get active transaction list
        let active_versions = Self::scan_active(&mut engine)?;
        // set current to active, note that current active list(get before) doesn't contain current version
        engine.set(MvccKey::TxnActive(new_version).encode()?, vec![])?;
        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: new_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        let mut delete_keys = Vec::new();
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        // end the life of engine iterator so that we can use engine later
        drop(iter);
        // clean txnwrite
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }
        // detete this trasction in active list
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
    }

    pub fn rollback(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        let mut delete_keys = Vec::new();
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    // version key
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "UNexpected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key); // txnwrite key
        }
        // end the life of engine iterator so that we can use engine later
        drop(iter);
        // clean txnwrite and version
        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }
        // detete this trasction in active list
        engine.delete(MvccKey::TxnActive(self.state.version).encode()?)
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
        let mut engine = self.engine.lock()?;
        // current version: 9
        // scan version 0 - 8
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev();
        // use rev to reverse iter
        // 从最新的版本开始读取，找到一个最新的可见的版本
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        Ok(None)
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
        .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
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
                        "Unexpected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }
        // 记录这个 version 写入了哪些 key，用于回滚事务
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode()?,
            vec![],
        )?;

        // 写入实际的 key value 数据
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode()?,
            bincode::serialize(&value)?,
        )?;
        Ok(())
    }

    // check data start by table name as prefix
    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        // 原始值           编码后
        // 97 98 99     -> 97 98 99 0 0
        // 前缀原始值        前缀编码后
        // 97 98        -> 97 98 0 0         -> 97 98
        // 去掉最后的 [0, 0] 后缀
        enc_prefix.truncate(enc_prefix.len() - 2);

        let mut iter = eng.scan_prefix(enc_prefix);
        let mut results = BTreeMap::new();
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                // update key if key is duolicate, so we always have the latest visivle key version
                MvccKey::Version(raw_key, version) => {
                    if self.state.is_visible(version) {
                        match bincode::deserialize(&value)? {
                            Some(raw_value) => results.insert(raw_key, raw_value),
                            // if value is none, delete that key
                            None => results.remove(&raw_key),
                        };
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexepected key {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
        }

        Ok(results
            .into_iter()
            .map(|(key, value)| ScanResult { key, value })
            .collect())
    }

    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode()?);
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

#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, engine::Engine, memory::MemoryEngine},
    };

    use super::Mvcc;

    // 1. Get
    fn get(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx1.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx1.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        get(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 2. Get Isolation
    fn get_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val2".to_vec())?;

        let tx2 = mvcc.begin()?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"key2".to_vec(), b"val4".to_vec())?;
        tx3.delete(b"key3".to_vec())?;
        tx3.commit()?;

        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, Some(b"val4".to_vec()));

        Ok(())
    }
    #[test]
    fn test_get_isolation() -> Result<()> {
        get_isolation(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 3. scan prefix
    fn scan_prefix(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                super::ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![super::ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_prefix() -> Result<()> {
        scan_prefix(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_prefix(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 4. scan isolation
    fn scan_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"aabb".to_vec(), b"val1".to_vec())?;
        tx.set(b"abcc".to_vec(), b"val2".to_vec())?;
        tx.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        tx.set(b"acca".to_vec(), b"val4".to_vec())?;
        tx.set(b"aaca".to_vec(), b"val5".to_vec())?;
        tx.set(b"bcca".to_vec(), b"val6".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx2.set(b"acca".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"aabb".to_vec(), b"val1-1".to_vec())?;

        let tx3 = mvcc.begin()?;
        tx3.set(b"bbaa".to_vec(), b"val3-1".to_vec())?;
        tx3.delete(b"bcca".to_vec())?;
        tx3.commit()?;

        let iter1 = tx1.scan_prefix(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = tx1.scan_prefix(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                super::ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                super::ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = tx1.scan_prefix(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![super::ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_isolation() -> Result<()> {
        scan_isolation(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 5. set
    fn set(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val3-2".to_vec())?;

        tx2.set(b"key3".to_vec(), b"val4-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val5-1".to_vec())?;

        tx1.commit()?;
        tx2.commit()?;

        let tx = mvcc.begin()?;
        assert_eq!(tx.get(b"key1".to_vec())?, Some(b"val1-1".to_vec()));
        assert_eq!(tx.get(b"key2".to_vec())?, Some(b"val3-2".to_vec()));
        assert_eq!(tx.get(b"key3".to_vec())?, Some(b"val4-1".to_vec()));
        assert_eq!(tx.get(b"key4".to_vec())?, Some(b"val5-1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        set(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 6. set conflict
    fn set_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key2".to_vec(), b"val3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val4".to_vec())?;
        tx.set(b"key4".to_vec(), b"val5".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key1".to_vec(), b"val1-2".to_vec())?;

        assert_eq!(
            tx2.set(b"key1".to_vec(), b"val1-3".to_vec()),
            Err(super::Error::WriteConflict)
        );

        let tx3 = mvcc.begin()?;
        tx3.set(b"key5".to_vec(), b"val6".to_vec())?;
        tx3.commit()?;

        assert_eq!(
            tx1.set(b"key5".to_vec(), b"val6-1".to_vec()),
            Err(super::Error::WriteConflict)
        );

        tx1.commit()?;
        Ok(())
    }

    #[test]
    fn test_set_conflict() -> Result<()> {
        set_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 7. delete
    fn delete(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.delete(b"key2".to_vec())?;
        tx.delete(b"key3".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        assert_eq!(tx1.get(b"key2".to_vec())?, None);

        let iter = tx1.scan_prefix(b"ke".to_vec())?;
        assert_eq!(
            iter,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3-1".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        delete(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 8. delete conflict
    fn delete_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;
        tx1.delete(b"key1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;

        assert_eq!(
            tx2.delete(b"key1".to_vec()),
            Err(super::Error::WriteConflict)
        );
        assert_eq!(
            tx2.delete(b"key2".to_vec()),
            Err(super::Error::WriteConflict)
        );

        Ok(())
    }

    #[test]
    fn test_delete_conflict() -> Result<()> {
        delete_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 9. dirty read
    fn dirty_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_dirty_read() -> Result<()> {
        dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        dirty_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 10. unrepeatable read
    fn unrepeatable_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        tx2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        tx2.commit()?;
        assert_eq!(tx1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_unrepeatable_read() -> Result<()> {
        unrepeatable_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        unrepeatable_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 11. phantom read
    fn phantom_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        let tx2 = mvcc.begin()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );

        tx2.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx2.set(b"key4".to_vec(), b"val4".to_vec())?;
        tx2.commit()?;

        let iter1 = tx1.scan_prefix(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                super::ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                super::ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                super::ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_phantom_read() -> Result<()> {
        phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        phantom_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 12. rollback
    fn rollback(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let tx = mvcc.begin()?;
        tx.set(b"key1".to_vec(), b"val1".to_vec())?;
        tx.set(b"key2".to_vec(), b"val2".to_vec())?;
        tx.set(b"key3".to_vec(), b"val3".to_vec())?;
        tx.commit()?;

        let tx1 = mvcc.begin()?;
        tx1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        tx1.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        tx1.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        tx1.rollback()?;

        let tx2 = mvcc.begin()?;
        assert_eq!(tx2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(tx2.get(b"key2".to_vec())?, Some(b"val2".to_vec()));
        assert_eq!(tx2.get(b"key3".to_vec())?, Some(b"val3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()> {
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        rollback(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}

