// LSM-tree 存储引擎
//
// 写路径：追加 WAL → 写入 memtable（BTreeMap）→ 超过阈值后 flush 成 L0 的 SSTable
// 读路径：memtable → L0（新到旧逐个查）→ L1+（每层不重叠，二分定位到表）
// compaction：L0 表数量超限时与 L1 归并；L1+ 层大小超限时（逐层 ×10）与下一层归并，
//             每层是一个不重叠的 sorted run，合并到最底层时丢弃墓碑
//
// 简化之处（教学范围）：
// - 没有 manifest 文件，SSTable 的层级和新旧关系编码在文件名 {id:08}-l{level}.sst 中，
//   id 全局单调递增，因此 id 大小顺序即数据新旧顺序
// - compaction 完成后先重命名新表再删除旧表，中间崩溃可能残留旧表（读取仍正确，
//   但底层墓碑回收后崩溃可能让被删除的 key 复活）
// - WAL 不做逐条 fsync，掉电可能丢失最近的写入（与 DiskEngine 行为一致）

pub mod bloom;
pub mod merge;
pub mod sstable;

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::{Bound, RangeBounds},
    path::PathBuf,
};

use fs4::FileExt;

use crate::error::{Error, Result};

use super::engine::{Engine, EngineIterator};
use merge::{MergeIterator, Source};
use sstable::{SsTable, SsTableBuilder};

pub struct Config {
    // memtable 超过该大小后 flush 成 SSTable
    pub memtable_size: usize,
    // SSTable data block 大小
    pub block_size: usize,
    // compaction 输出文件的切分大小
    pub sstable_target_size: usize,
    // L0 文件数超过该值触发 compaction
    pub l0_compact_threshold: usize,
    // L1 的大小上限，此后每层 ×10
    pub level_base_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            memtable_size: 4 << 20,        // 4 MB
            block_size: 4 << 10,           // 4 KB
            sstable_target_size: 2 << 20,  // 2 MB
            l0_compact_threshold: 4,
            level_base_size: 8 << 20,      // 8 MB
        }
    }
}

pub struct LsmEngine {
    dir: PathBuf,
    cfg: Config,
    // memtable，value 为 None 表示墓碑
    memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    memtable_bytes: usize,
    wal: Wal,
    // levels[0] 按新到旧排列（可重叠）；levels[1..] 按 first_key 排列（不重叠）
    levels: Vec<Vec<SsTable>>,
    next_id: u64,
    // 目录排他锁，保证同时只有一个进程打开该数据库
    _lock: File,
}

impl LsmEngine {
    pub fn new(dir: PathBuf) -> Result<Self> {
        Self::new_with_config(dir, Config::default())
    }

    pub fn new_with_config(dir: PathBuf, cfg: Config) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;

        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock_exclusive()?;

        // 扫描目录恢复各层的 SSTable，清理上次崩溃残留的临时文件
        let mut tables = Vec::new();
        let mut next_id = 1;
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if name.ends_with(".tmp") {
                std::fs::remove_file(&path)?;
                continue;
            }
            if let Some(rest) = name.strip_suffix(".sst") {
                let (id, level) = rest
                    .split_once("-l")
                    .and_then(|(i, l)| Some((i.parse::<u64>().ok()?, l.parse::<usize>().ok()?)))
                    .ok_or(Error::Internal(format!("invalid sstable file name {}", name)))?;
                next_id = next_id.max(id + 1);
                tables.push(SsTable::open(path, id, level)?);
            }
        }

        let max_level = tables.iter().map(|t| t.level).max().unwrap_or(0);
        let mut levels: Vec<Vec<SsTable>> = (0..=max_level + 1).map(|_| Vec::new()).collect();
        for t in tables {
            let level = t.level;
            levels[level].push(t);
        }
        levels[0].sort_by(|a, b| b.id.cmp(&a.id)); // L0 新到旧
        for level in levels[1..].iter_mut() {
            level.sort_by(|a, b| a.first_key.cmp(&b.first_key));
        }

        // 重放 WAL，恢复上次未 flush 的 memtable
        let mut wal = Wal::open(dir.join("wal.log"))?;
        let mut memtable = BTreeMap::new();
        let mut memtable_bytes = 0;
        for (key, value) in wal.replay()? {
            memtable_bytes += key.len() + value.as_ref().map_or(0, |v| v.len());
            memtable.insert(key, value);
        }

        Ok(Self {
            dir,
            cfg,
            memtable,
            memtable_bytes,
            wal,
            levels,
            next_id,
            _lock: lock,
        })
    }

    fn write_inner(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        self.wal.append(&key, value.as_deref())?;
        self.memtable_bytes += key.len() + value.as_ref().map_or(0, |v| v.len());
        self.memtable.insert(key, value);
        if self.memtable_bytes >= self.cfg.memtable_size {
            self.flush()?;
        }
        Ok(())
    }

    // 把 memtable 写成 L0 的一个 SSTable，然后清空 WAL
    fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut builder = SsTableBuilder::new(&self.dir, id, 0, self.cfg.block_size)?;
        for (key, value) in self.memtable.iter() {
            builder.add(key, value.as_deref())?;
        }
        let table = builder.finish()?;
        self.levels[0].insert(0, table); // 最新的放最前面

        self.memtable.clear();
        self.memtable_bytes = 0;
        // SSTable 已落盘，WAL 中的数据可以安全丢弃
        self.wal.reset()?;

        self.maybe_compact()
    }

    fn maybe_compact(&mut self) -> Result<()> {
        loop {
            if self.levels[0].len() > self.cfg.l0_compact_threshold {
                self.compact(0)?;
                continue;
            }
            let mut compacted = false;
            for i in 1..self.levels.len() {
                let max_size = self.cfg.level_base_size * 10usize.pow(i as u32 - 1);
                if self.levels[i].iter().map(|t| t.size as usize).sum::<usize>() > max_size {
                    self.compact(i)?;
                    compacted = true;
                    break;
                }
            }
            if !compacted {
                return Ok(());
            }
        }
    }

    // 把第 level 层与第 level+1 层归并，输出一组新的不重叠 SSTable 作为第 level+1 层
    fn compact(&mut self, level: usize) -> Result<()> {
        while self.levels.len() <= level + 1 {
            self.levels.push(Vec::new());
        }
        // 输出层之下没有数据时，墓碑不再屏蔽任何旧版本，可以直接丢弃
        let bottommost = self.levels[level + 2..].iter().all(|l| l.is_empty());

        let mut inputs = std::mem::take(&mut self.levels[level]);
        inputs.extend(std::mem::take(&mut self.levels[level + 1]));

        // id 全局单调递增，优先级用 id 即可反映数据新旧
        let sources = inputs
            .iter_mut()
            .map(|t| {
                let priority = t.id;
                Source::new(
                    Box::new(t.iter_range(Bound::Unbounded, Bound::Unbounded)) as Box<_>,
                    priority,
                )
            })
            .collect();
        let mut merged = MergeIterator::new(sources);

        let mut outputs = Vec::new();
        let mut builder: Option<SsTableBuilder> = None;
        while let Some(item) = merged.next() {
            let (key, value) = item?;
            if value.is_none() && bottommost {
                continue;
            }
            if builder.is_none() {
                let id = self.next_id;
                self.next_id += 1;
                builder = Some(SsTableBuilder::new(
                    &self.dir,
                    id,
                    level + 1,
                    self.cfg.block_size,
                )?);
            }
            let b = builder.as_mut().unwrap();
            b.add(&key, value.as_deref())?;
            if b.estimated_size() >= self.cfg.sstable_target_size {
                outputs.push(builder.take().unwrap().finish()?);
            }
        }
        if let Some(b) = builder {
            outputs.push(b.finish()?);
        }
        drop(merged);

        // 新表已全部落盘，删除旧表
        for t in inputs {
            t.remove()?;
        }
        self.levels[level + 1] = outputs;
        Ok(())
    }
}

impl Engine for LsmEngine {
    type EngineIterator<'a> = LsmIterator<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // memtable 中的记录（包括墓碑）是最新版本
        if let Some(value) = self.memtable.get(&key) {
            return Ok(value.clone());
        }
        // L0 表之间可能重叠，按新到旧逐个查
        for table in self.levels[0].iter_mut() {
            if let Some(value) = table.get(&key)? {
                return Ok(value);
            }
        }
        // L1+ 每层不重叠，二分定位到唯一可能包含该 key 的表
        for level in self.levels[1..].iter_mut() {
            let pos = level.partition_point(|t| t.first_key.as_slice() <= key.as_slice());
            if pos == 0 {
                continue;
            }
            if let Some(value) = level[pos - 1].get(&key)? {
                return Ok(value);
            }
        }
        Ok(None)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        // 写入墓碑，真正的删除发生在最底层 compaction
        self.write_inner(key, None)
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let mut sources = Vec::new();
        // memtable 永远是最新的数据
        let mem_iter = self
            .memtable
            .range((start.clone(), end.clone()))
            .map(|(k, v)| Ok((k.clone(), v.clone())));
        sources.push(Source::new(Box::new(mem_iter) as Box<_>, u64::MAX));

        for table in self.levels.iter_mut().flat_map(|level| level.iter_mut()) {
            let priority = table.id;
            let iter = table.iter_range(start.clone(), end.clone());
            sources.push(Source::new(Box::new(iter) as Box<_>, priority));
        }

        LsmIterator {
            inner: MergeIterator::new(sources),
        }
    }
}

// 引擎对外的扫描迭代器：过滤掉归并结果中的墓碑
pub struct LsmIterator<'a> {
    inner: MergeIterator<'a>,
}

impl<'a> EngineIterator for LsmIterator<'a> {}

impl<'a> Iterator for LsmIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Ok((_, None)) => continue,
                Ok((key, Some(value))) => return Some(Ok((key, value))),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<'a> DoubleEndedIterator for LsmIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next_back()? {
                Ok((_, None)) => continue,
                Ok((key, Some(value))) => return Some(Ok((key, value))),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// 预写日志：记录格式与 SSTable 块内记录一致
// key_len(4) + val_len(4, -1 表示墓碑) + key + value
struct Wal {
    file: File,
}

impl Wal {
    fn open(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self { file })
    }

    fn append(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let mut writer = BufWriter::new(&self.file);
        writer.write_all(&(key.len() as u32).to_be_bytes())?;
        writer.write_all(&value.map_or(-1i32, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn replay(&mut self) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let file_size = self.file.metadata()?.len();
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&self.file);

        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < file_size {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let key_len = u32::from_be_bytes(len_buf);
            reader.read_exact(&mut len_buf)?;
            let val_len = i32::from_be_bytes(len_buf);

            let mut key = vec![0; key_len as usize];
            reader.read_exact(&mut key)?;
            let value = if val_len >= 0 {
                let mut v = vec![0; val_len as usize];
                reader.read_exact(&mut v)?;
                Some(v)
            } else {
                None
            };
            offset += 8 + key_len as u64 + val_len.max(0) as u64;
            entries.push((key, value));
        }
        Ok(entries)
    }

    // memtable flush 完成后清空日志
    fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    // 小配置，用少量数据触发大量 flush 和 compaction
    fn small_config() -> Config {
        Config {
            memtable_size: 256,
            block_size: 64,
            sstable_target_size: 512,
            l0_compact_threshold: 2,
            level_base_size: 1024,
        }
    }

    fn key(i: u32) -> Vec<u8> {
        format!("key-{:04}", i).into_bytes()
    }

    fn value(i: u32) -> Vec<u8> {
        format!("value-{}", i).into_bytes()
    }

    #[test]
    fn test_lsm_flush_and_compact() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;

        // 写入远超 memtable 大小的数据，覆盖写 + 删除
        for i in 0..500 {
            eng.set(key(i), value(i))?;
        }
        for i in (0..500).step_by(2) {
            eng.set(key(i), value(i + 10000))?; // 覆盖偶数 key
        }
        for i in (0..500).step_by(5) {
            eng.delete(key(i))?; // 删除 5 的倍数
        }

        // 必然发生过 flush 和 compaction
        assert!(eng.next_id > 1);
        assert!(eng.levels.len() > 1);

        // 验证读取
        assert_eq!(eng.get(key(0))?, None); // 被删除
        assert_eq!(eng.get(key(1))?, Some(value(1)));
        assert_eq!(eng.get(key(2))?, Some(value(10002))); // 被覆盖
        assert_eq!(eng.get(key(4))?, Some(value(10004)));
        assert_eq!(eng.get(key(5))?, None);
        assert_eq!(eng.get(b"not-exist".to_vec())?, None);

        // 验证扫描：结果有序、无墓碑、无重复
        let items = eng.scan(..).collect::<Result<Vec<_>>>()?;
        let expect = 500 - 100; // 删除了 100 个
        assert_eq!(items.len(), expect);
        for w in items.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
        Ok(())
    }

    #[test]
    fn test_lsm_recovery() -> Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;
            for i in 0..300 {
                eng.set(key(i), value(i))?;
            }
            for i in 0..100 {
                eng.delete(key(i))?;
            }
            // 不显式 flush，直接 drop：未落盘部分靠 WAL 恢复
        }

        let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;
        assert_eq!(eng.get(key(0))?, None);
        assert_eq!(eng.get(key(100))?, Some(value(100)));
        assert_eq!(eng.get(key(299))?, Some(value(299)));
        let items = eng.scan(..).collect::<Result<Vec<_>>>()?;
        assert_eq!(items.len(), 200);
        Ok(())
    }

    #[test]
    fn test_lsm_shadowing_across_levels() -> Result<()> {
        // 同一个 key 的多个版本分布在 memtable / L0 / L1，读到的必须是最新版本
        let dir = tempfile::tempdir()?;
        let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;

        for round in 0..5 {
            for i in 0..100 {
                eng.set(key(i), value(i + round * 1000))?;
            }
        }
        for i in 0..100 {
            assert_eq!(eng.get(key(i))?, Some(value(i + 4000)));
        }
        let items = eng.scan(..).collect::<Result<Vec<_>>>()?;
        assert_eq!(items.len(), 100);
        assert_eq!(items[0].1, value(4000));
        Ok(())
    }

    #[test]
    fn test_lsm_scan_bidirectional() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;
        for i in 0..100 {
            eng.set(key(i), value(i))?;
        }

        // 反向扫描
        let mut iter = eng.scan(key(10)..key(20)).rev();
        assert_eq!(iter.next().transpose()?.unwrap().0, key(19));
        assert_eq!(iter.next().transpose()?.unwrap().0, key(18));
        drop(iter);

        // 双向交替
        let mut iter = eng.scan(key(10)..=key(12));
        assert_eq!(iter.next().transpose()?.unwrap().0, key(10));
        assert_eq!(iter.next_back().transpose()?.unwrap().0, key(12));
        assert_eq!(iter.next().transpose()?.unwrap().0, key(11));
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
        Ok(())
    }

    #[test]
    fn test_lsm_tombstone_gc() -> Result<()> {
        // 写入后全部删除并触发底层 compaction，墓碑应该被回收
        let dir = tempfile::tempdir()?;
        let mut eng = LsmEngine::new_with_config(dir.path().to_path_buf(), small_config())?;
        for i in 0..200 {
            eng.set(key(i), value(i))?;
        }
        for i in 0..200 {
            eng.delete(key(i))?;
        }
        // 强制把 memtable 中剩余的墓碑刷下去，再从上往下逐层级联合并，
        // 墓碑在合并到最底层时被回收
        eng.flush()?;
        let mut i = 0;
        while i + 1 < eng.levels.len() {
            eng.compact(i)?;
            i += 1;
        }

        let items = eng.scan(..).collect::<Result<Vec<_>>>()?;
        assert!(items.is_empty());
        // 最底层的数据里不应该再有任何记录（墓碑已回收）
        let total: usize = eng.levels.iter().map(|l| l.len()).sum();
        assert_eq!(total, 0);
        Ok(())
    }

    #[test]
    fn test_lsm_sql_end_to_end() -> Result<()> {
        // LSM 引擎接入 SQL 层跑通完整链路
        use crate::sql::engine::kv::KVEngine;
        use crate::sql::engine::Engine as SqlEngine;
        use crate::sql::executor::ResultSet;

        let dir = tempfile::tempdir()?;
        let eng = KVEngine::new(LsmEngine::new_with_config(
            dir.path().to_path_buf(),
            small_config(),
        )?);
        let mut session = eng.session()?;

        session.execute("create table t (id int, name text default 'none');")?;
        for i in 0..50 {
            session.execute(&format!("insert into t values({}, 'name-{}');", i, i))?;
        }
        match session.execute("select * from t;")? {
            ResultSet::Scan { rows, .. } => assert_eq!(rows.len(), 50),
            other => panic!("unexpected result: {:?}", other),
        }
        Ok(())
    }
}
