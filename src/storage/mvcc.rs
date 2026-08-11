use std::{
    collections::HashSet,
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

// 活跃事务集的内存缓存：None 表示尚未从存储加载
// 每次 begin 都扫描存储的话，在 LSM 引擎上会随着 TxnActive 墓碑累积越来越慢
type ActiveCache = Arc<Mutex<Option<HashSet<Version>>>>;

pub struct Mvcc<E: Engine> {
    engine: Arc<Mutex<E>>,
    active: ActiveCache,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            active: self.active.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone(), self.active.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    active: ActiveCache,
    state: TransactionState,
    // 本事务是否写过数据；只读事务提交时可以跳过 TxnWrite 扫描
    wrote: std::cell::Cell<bool>,
}

// 事务状态
pub struct TransactionState {
    // 当前事务的版本号
    pub version: Version,
    // 当前活跃事务版本列表
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnAcvtive(Version),
    TxnWrite(Version, #[serde(with = "serde_bytes")] Vec<u8>),
    Version(#[serde(with = "serde_bytes")] Vec<u8>, Version),
}

// NextVersion 0
// TxnAcvtive 1-100 1-101 1-102
// Version key1-101 key2-101

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
    TxnAcvtive,
    TxnWrite(Version),
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

impl<E: Engine> MvccTransaction<E> {
    // 开启事务
    pub fn begin(eng: Arc<Mutex<E>>, active: ActiveCache) -> Result<Self> {
        // 获取存储引擎
        let mut engine = eng.lock()?;
        // 获取最新的版本号
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };
        // 保存下一个 version
        engine.set(
            MvccKey::NextVersion.encode()?,
            bincode::serialize(&(next_version + 1))?,
        )?;

        // 获取当前活跃的事务列表：首次从存储加载（含崩溃残留的事务），
        // 之后由内存缓存增量维护，避免每次 begin 都扫描存储
        // （在 LSM 引擎上，已删除的 TxnActive 记录是墓碑，扫描会越来越慢）
        let mut active_guard = active.lock()?;
        if active_guard.is_none() {
            *active_guard = Some(Self::scan_active(&mut engine)?);
        }
        let active_set = active_guard.as_mut().unwrap();
        let active_versions = active_set.clone();

        // 当前事务加入到活跃事务列表中（持久化 + 缓存同步更新）
        engine.set(MvccKey::TxnAcvtive(next_version).encode()?, vec![])?;
        active_set.insert(next_version);
        drop(active_guard);

        Ok(Self {
            engine: eng.clone(),
            active,
            state: TransactionState {
                version: next_version,
                active_versions,
            },
            wrote: std::cell::Cell::new(false),
        })
    }

    // 提交事务
    pub fn commit(&self) -> Result<()> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;

        // 只读事务没有 TxnWrite 记录，跳过扫描
        if self.wrote.get() {
            let mut delete_keys = Vec::new();
            // 找到这个当前事务的 TxnWrite 信息
            let mut iter =
                engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
            while let Some((key, _)) = iter.next().transpose()? {
                delete_keys.push(key);
            }
            drop(iter);

            for key in delete_keys.into_iter() {
                engine.delete(key)?;
            }
        }

        // 从活跃事务列表中删除（持久化 + 缓存同步更新）
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)?;
        if let Some(set) = self.active.lock()?.as_mut() {
            set.remove(&self.state.version);
        }
        Ok(())
    }

    // 回滚事务
    pub fn rollback(&self) -> Result<()> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // 找到这个当前事务的 TxnWrite 信息
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )))
                }
            }
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // 从活跃事务列表中删除（持久化 + 缓存同步更新）
        engine.delete(MvccKey::TxnAcvtive(self.state.version).encode()?)?;
        if let Some(set) = self.active.lock()?.as_mut() {
            set.remove(&self.state.version);
        }
        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;

        // version: 9
        // 扫描的 version 的范围应该是 0-8
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev();
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

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut enc_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        // 原始值           编码后
        // 97 98 99     -> 97 98 99 0 0
        // 前缀原始值        前缀编码后
        // 97 98        -> 97 98 0 0         -> 97 98
        // 去掉最后的 [0, 0] 后缀
        enc_prefix.truncate(enc_prefix.len() - 2);

        // 引擎扫描按 key 有序，同一个 raw key 的所有版本相邻且版本号递增，
        // 因此可以流式处理：跟踪当前 key 的最新可见版本，key 变化时输出，
        // 每个 key 只做一次 value 解码，避免中间 BTreeMap 和逐版本解码
        let mut iter = eng.scan_prefix(enc_prefix);
        let mut results = Vec::new();
        // (raw key, 未解码的最新可见版本)
        let mut current: Option<(Vec<u8>, Vec<u8>)> = None;

        let mut emit = |current: &mut Option<(Vec<u8>, Vec<u8>)>,
                        results: &mut Vec<ScanResult>|
         -> Result<()> {
            if let Some((key, value)) = current.take() {
                // 墓碑（删除标记）不输出
                if let Some(value) = bincode::deserialize::<Option<Vec<u8>>>(&value)? {
                    results.push(ScanResult { key, value });
                }
            }
            Ok(())
        };

        while let Some((key, value)) = iter.next().transpose()? {
            // 扫描热路径：手写解码代替 serde 反序列化
            let (raw_key, version) = decode_version_key(&key)?;
            if !self.state.is_visible(version) {
                continue;
            }
            match &mut current {
                // 同一个 key 的更新版本，覆盖候选值
                Some((ck, cv)) if *ck == raw_key => *cv = value,
                Some(_) => {
                    emit(&mut current, &mut results)?;
                    current = Some((raw_key, value));
                }
                None => current = Some((raw_key, value)),
            }
        }
        emit(&mut current, &mut results)?;

        Ok(results)
    }

    // 更新/删除数据
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;

        // 检测冲突
        //  3 4 5
        //  6
        //  key1-3 key2-4 key3-5
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode()?;
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        //  当前活跃事务列表 3 4 5
        //  当前事务 6
        // 只需要判断最后一个版本号
        // 1. key 按照顺序排列，扫描出的结果是从小到大的
        // 2. 假如有新的的事务修改了这个 key，比如 10，修改之后 10 提交了，那么 6 再修改这个 key 就是冲突的
        // 3. 如果是当前活跃事务修改了这个 key，比如 4，那么事务 5 就不可能修改这个 key
        if let Some((k, _)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // 检测这个 version 是否是可见的
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
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
        self.wrote.set(true);
        Ok(())
    }

    // 扫描获取当前活跃事务列表
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnAcvtive.encode()?);
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnAcvtive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
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

// MvccKey::Version 的手写解码，扫描热路径避免 serde 反序列化的开销
// 编码格式（见 keycode）：[变体索引 3][key，0x00 转义为 0x00 0xFF][0x00 0x00][8 字节大端 version]
fn decode_version_key(key: &[u8]) -> Result<(Vec<u8>, Version)> {
    if key.first() != Some(&3) {
        return Err(Error::Internal("unexpected key in version scan".into()));
    }
    let mut raw = Vec::with_capacity(key.len());
    let mut i = 1;
    loop {
        match key.get(i) {
            Some(0) => match key.get(i + 1) {
                // 0x00 0x00 是结束符
                Some(0) => {
                    i += 2;
                    break;
                }
                // 0x00 0xFF 是转义的 0x00
                Some(255) => {
                    raw.push(0);
                    i += 2;
                }
                _ => return Err(Error::Internal("malformed version key".into())),
            },
            Some(b) => {
                raw.push(*b);
                i += 1;
            }
            None => return Err(Error::Internal("malformed version key".into())),
        }
    }
    let bytes: [u8; 8] = key
        .get(i..i + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::Internal("malformed version key".into()))?;
    Ok((raw, Version::from_be_bytes(bytes)))
}

#[cfg(test)]
mod decode_tests {
    use super::*;

    #[test]
    fn test_decode_version_key_matches_serde() -> Result<()> {
        // 手写解码必须与 serde 编码严格互逆，包括含 0x00 的 key
        for raw in [
            b"normal".to_vec(),
            vec![0x00, 0x01, 0x00],
            vec![],
            vec![0xff, 0x00],
        ] {
            for version in [0u64, 1, 42, u64::MAX] {
                let encoded = MvccKey::Version(raw.clone(), version).encode()?;
                assert_eq!(decode_version_key(&encoded)?, (raw.clone(), version));
            }
        }
        Ok(())
    }
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
