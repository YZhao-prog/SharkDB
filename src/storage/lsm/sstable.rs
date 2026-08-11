// SSTable（Sorted String Table）：不可变的有序数据文件
//
// 文件格式（LevelDB 风格的简化版）：
// +----------------+----------------+---------------+----------------+
// |   data blocks  |  index block   |  bloom block  |  footer (40B)  |
// +----------------+----------------+---------------+----------------+
//
// - data block：约 block_size 大小，内部是连续的记录
//   记录格式：key_len(4) + val_len(4, -1 表示墓碑) + key + value
// - index block：last_key + 每个 block 的 (first_key, offset, len)，
//   打开文件时载入内存，点查时二分定位到 block 再读盘
// - bloom block：整个文件所有 key 的 Bloom 过滤器
// - footer：index_off(8) + index_len(8) + bloom_off(8) + bloom_len(8) + magic(8)

use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    ops::Bound,
    path::PathBuf,
};

use crate::error::{Error, Result};

use super::bloom::{self, BloomFilter};
use super::merge::MergeItem;

const FOOTER_SIZE: u64 = 40;
const MAGIC: u64 = 0x53514C44424C534D; // "SQLDBLSM"

// 块内记录：key + 值（None 表示墓碑）
pub type BlockEntry = (Vec<u8>, Option<Vec<u8>>);

// 稀疏索引项，每个 data block 一条
pub struct IndexEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
}

pub struct SsTable {
    pub id: u64,
    pub level: usize,
    pub path: PathBuf,
    pub size: u64,
    file: File,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}

impl SsTable {
    // 打开一个已存在的 SSTable，读取 footer、索引和 Bloom 过滤器到内存
    pub fn open(path: PathBuf, id: u64, level: usize) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let size = file.metadata()?.len();

        // 读取 footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;
        let index_off = u64::from_be_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_be_bytes(footer[8..16].try_into().unwrap());
        let bloom_off = u64::from_be_bytes(footer[16..24].try_into().unwrap());
        let bloom_len = u64::from_be_bytes(footer[24..32].try_into().unwrap());
        let magic = u64::from_be_bytes(footer[32..40].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::Internal(format!(
                "invalid sstable file {}",
                path.display()
            )));
        }

        // 读取索引块
        file.seek(SeekFrom::Start(index_off))?;
        let mut buf = vec![0; index_len as usize];
        file.read_exact(&mut buf)?;
        let (index, last_key) = Self::decode_index(&buf)?;

        // 读取 Bloom 过滤器
        file.seek(SeekFrom::Start(bloom_off))?;
        let mut buf = vec![0; bloom_len as usize];
        file.read_exact(&mut buf)?;
        let bloom = BloomFilter::decode(&buf);

        let first_key = index
            .first()
            .map(|e| e.first_key.clone())
            .unwrap_or_default();
        Ok(Self {
            id,
            level,
            path,
            size,
            file,
            index,
            bloom,
            first_key,
            last_key,
        })
    }

    // 点查：Ok(None) 表示此表不含该 key，Ok(Some(None)) 表示墓碑
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        if key < self.first_key.as_slice() || key > self.last_key.as_slice() {
            return Ok(None);
        }
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        // 二分找到可能包含该 key 的 block（最后一个 first_key <= key 的块）
        let pos = self.index.partition_point(|e| e.first_key.as_slice() <= key);
        if pos == 0 {
            return Ok(None);
        }
        let entries = read_block(&mut self.file, &self.index[pos - 1])?;
        for (k, v) in entries {
            if k == key {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    // 范围扫描迭代器，块按需读取，支持双向迭代
    pub fn iter_range(&mut self, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>) -> SsTableIter<'_> {
        // 起始块：最后一个 first_key <= start 的块（start 可能落在该块中间）
        let lo = match &start {
            Bound::Included(k) | Bound::Excluded(k) => self
                .index
                .partition_point(|e| e.first_key.as_slice() <= k.as_slice())
                .saturating_sub(1),
            Bound::Unbounded => 0,
        };
        // 结束块：first_key 超出 end 的块不需要读取
        let hi = match &end {
            Bound::Included(k) => self
                .index
                .partition_point(|e| e.first_key.as_slice() <= k.as_slice()),
            Bound::Excluded(k) => self
                .index
                .partition_point(|e| e.first_key.as_slice() < k.as_slice()),
            Bound::Unbounded => self.index.len(),
        };
        let hi = hi.max(lo);

        SsTableIter {
            file: &mut self.file,
            blocks: &self.index[lo..hi],
            start,
            end,
            front_buf: VecDeque::new(),
            back_buf: VecDeque::new(),
        }
    }

    // 删除底层文件（compaction 合并完成后调用）
    pub fn remove(self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }

    // 索引块格式：last_key_len(4) + last_key + count(4)
    //           + count 个 (first_key_len(4) + first_key + offset(8) + len(4))
    fn decode_index(buf: &[u8]) -> Result<(Vec<IndexEntry>, Vec<u8>)> {
        let mut pos = 0;
        let last_key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let last_key = buf[pos..pos + last_key_len].to_vec();
        pos += last_key_len;
        let count = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let mut index = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let first_key = buf[pos..pos + key_len].to_vec();
            pos += key_len;
            let offset = u64::from_be_bytes(buf[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
            pos += 4;
            index.push(IndexEntry {
                first_key,
                offset,
                len,
            });
        }
        Ok((index, last_key))
    }
}

// 读取并解析一个 data block
fn read_block(file: &mut File, entry: &IndexEntry) -> Result<VecDeque<BlockEntry>> {
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut buf = vec![0; entry.len as usize];
    file.read_exact(&mut buf)?;

    let mut entries = VecDeque::new();
    let mut pos = 0;
    while pos < buf.len() {
        let key_len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let val_len = i32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;
        let value = if val_len >= 0 {
            let v = buf[pos..pos + val_len as usize].to_vec();
            pos += val_len as usize;
            Some(v)
        } else {
            None
        };
        entries.push_back((key, value));
    }
    Ok(entries)
}

fn in_range(key: &[u8], start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> bool {
    let lo_ok = match start {
        Bound::Included(s) => key >= s.as_slice(),
        Bound::Excluded(s) => key > s.as_slice(),
        Bound::Unbounded => true,
    };
    let hi_ok = match end {
        Bound::Included(e) => key <= e.as_slice(),
        Bound::Excluded(e) => key < e.as_slice(),
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
}

// SSTable 范围扫描迭代器：
// blocks 是待读取的块列表，前后两端各有一个已解析的块缓冲，
// 保证任意时刻内存中最多持有两个块的数据
pub struct SsTableIter<'a> {
    file: &'a mut File,
    blocks: &'a [IndexEntry],
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    front_buf: VecDeque<BlockEntry>,
    back_buf: VecDeque<BlockEntry>,
}

impl<'a> SsTableIter<'a> {
    // 读取一个块并按范围过滤；出错时终止迭代
    fn load(&mut self, entry: &IndexEntry) -> Result<VecDeque<BlockEntry>> {
        let entries = read_block(self.file, entry)?;
        Ok(entries
            .into_iter()
            .filter(|(k, _)| in_range(k, &self.start, &self.end))
            .collect())
    }
}

impl<'a> Iterator for SsTableIter<'a> {
    type Item = MergeItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.front_buf.pop_front() {
                return Some(Ok(item));
            }
            // 切片引用是 Copy 的，先拷贝出来再切分，避免借用冲突
            let blocks = self.blocks;
            match blocks.split_first() {
                Some((first, rest)) => {
                    self.blocks = rest;
                    match self.load(first) {
                        Ok(entries) => self.front_buf = entries,
                        Err(e) => {
                            self.blocks = &[];
                            return Some(Err(e));
                        }
                    }
                }
                // 所有块读完，剩余数据在后端缓冲中
                None => return self.back_buf.pop_front().map(Ok),
            }
        }
    }
}

impl<'a> DoubleEndedIterator for SsTableIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.back_buf.pop_back() {
                return Some(Ok(item));
            }
            let blocks = self.blocks;
            match blocks.split_last() {
                Some((last, rest)) => {
                    self.blocks = rest;
                    match self.load(last) {
                        Ok(entries) => self.back_buf = entries,
                        Err(e) => {
                            self.blocks = &[];
                            return Some(Err(e));
                        }
                    }
                }
                None => return self.front_buf.pop_back().map(Ok),
            }
        }
    }
}

// SSTable 构建器：按序追加记录，写满一个块就刷盘
// 先写到 .tmp 文件，finish 时 fsync 后原子重命名为正式文件
pub struct SsTableBuilder {
    path: PathBuf,
    tmp_path: PathBuf,
    id: u64,
    level: usize,
    writer: BufWriter<File>,
    block_size: usize,
    block_buf: Vec<u8>,
    block_first_key: Option<Vec<u8>>,
    index: Vec<IndexEntry>,
    offset: u64,
    key_hashes: Vec<u64>,
    last_key: Vec<u8>,
}

impl SsTableBuilder {
    pub fn new(dir: &PathBuf, id: u64, level: usize, block_size: usize) -> Result<Self> {
        let path = table_path(dir, id, level);
        let mut tmp_path = path.clone();
        tmp_path.set_extension("tmp");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        Ok(Self {
            path,
            tmp_path,
            id,
            level,
            writer: BufWriter::new(file),
            block_size,
            block_buf: Vec::with_capacity(block_size),
            block_first_key: None,
            index: Vec::new(),
            offset: 0,
            key_hashes: Vec::new(),
            last_key: Vec::new(),
        })
    }

    // 追加一条记录，key 必须严格递增
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if self.block_first_key.is_none() {
            self.block_first_key = Some(key.to_vec());
        }
        self.block_buf
            .extend_from_slice(&(key.len() as u32).to_be_bytes());
        self.block_buf
            .extend_from_slice(&value.map_or(-1i32, |v| v.len() as i32).to_be_bytes());
        self.block_buf.extend_from_slice(key);
        if let Some(v) = value {
            self.block_buf.extend_from_slice(v);
        }
        self.key_hashes.push(bloom::hash(key));
        self.last_key = key.to_vec();

        if self.block_buf.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    // 已写入的数据量估计，用于 compaction 时切分输出文件
    pub fn estimated_size(&self) -> usize {
        self.offset as usize + self.block_buf.len()
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        self.index.push(IndexEntry {
            first_key: self.block_first_key.take().unwrap(),
            offset: self.offset,
            len: self.block_buf.len() as u32,
        });
        self.writer.write_all(&self.block_buf)?;
        self.offset += self.block_buf.len() as u64;
        self.block_buf.clear();
        Ok(())
    }

    // 写入索引、Bloom 过滤器和 footer，落盘并重命名，返回可读的 SSTable
    pub fn finish(mut self) -> Result<SsTable> {
        self.flush_block()?;

        // 索引块
        let index_off = self.offset;
        let mut index_buf = Vec::new();
        index_buf.extend_from_slice(&(self.last_key.len() as u32).to_be_bytes());
        index_buf.extend_from_slice(&self.last_key);
        index_buf.extend_from_slice(&(self.index.len() as u32).to_be_bytes());
        for e in &self.index {
            index_buf.extend_from_slice(&(e.first_key.len() as u32).to_be_bytes());
            index_buf.extend_from_slice(&e.first_key);
            index_buf.extend_from_slice(&e.offset.to_be_bytes());
            index_buf.extend_from_slice(&e.len.to_be_bytes());
        }
        self.writer.write_all(&index_buf)?;

        // Bloom 块
        let bloom = BloomFilter::from_hashes(&self.key_hashes);
        let bloom_buf = bloom.encode();
        let bloom_off = index_off + index_buf.len() as u64;
        self.writer.write_all(&bloom_buf)?;

        // footer
        self.writer.write_all(&index_off.to_be_bytes())?;
        self.writer.write_all(&(index_buf.len() as u64).to_be_bytes())?;
        self.writer.write_all(&bloom_off.to_be_bytes())?;
        self.writer.write_all(&(bloom_buf.len() as u64).to_be_bytes())?;
        self.writer.write_all(&MAGIC.to_be_bytes())?;

        // 落盘后原子重命名，保证不会出现半成品的正式文件
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        std::fs::rename(&self.tmp_path, &self.path)?;

        let size = bloom_off + bloom_buf.len() as u64 + FOOTER_SIZE;
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let first_key = self
            .index
            .first()
            .map(|e| e.first_key.clone())
            .unwrap_or_default();
        Ok(SsTable {
            id: self.id,
            level: self.level,
            path: self.path,
            size,
            file,
            index: self.index,
            bloom,
            first_key,
            last_key: self.last_key,
        })
    }
}

pub fn table_path(dir: &PathBuf, id: u64, level: usize) -> PathBuf {
    dir.join(format!("{:08}-l{}.sst", id, level))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    fn build_table(dir: &PathBuf, n: u32) -> Result<SsTable> {
        let mut b = SsTableBuilder::new(dir, 1, 0, 64)?;
        for i in 0..n {
            let key = format!("key-{:04}", i).into_bytes();
            if i % 10 == 3 {
                b.add(&key, None)?; // 墓碑
            } else {
                b.add(&key, Some(format!("val-{}", i).as_bytes()))?;
            }
        }
        b.finish()
    }

    #[test]
    fn test_sstable_build_get() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut table = build_table(&dir.path().to_path_buf(), 100)?;

        assert_eq!(table.get(b"key-0000")?, Some(Some(b"val-0".to_vec())));
        assert_eq!(table.get(b"key-0003")?, Some(None)); // 墓碑
        assert_eq!(table.get(b"key-0099")?, Some(Some(b"val-99".to_vec())));
        assert_eq!(table.get(b"key-9999")?, None);
        assert_eq!(table.get(b"a")?, None);
        Ok(())
    }

    #[test]
    fn test_sstable_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let table = build_table(&dir.path().to_path_buf(), 100)?;
        let path = table.path.clone();
        drop(table);

        // 从磁盘重新打开，验证索引和数据都能恢复
        let mut table = SsTable::open(path, 1, 0)?;
        assert_eq!(table.get(b"key-0050")?, Some(Some(b"val-50".to_vec())));
        assert_eq!(table.first_key, b"key-0000".to_vec());
        assert_eq!(table.last_key, b"key-0099".to_vec());
        Ok(())
    }

    #[test]
    fn test_sstable_scan() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut table = build_table(&dir.path().to_path_buf(), 100)?;

        // 正向范围扫描（包含墓碑）
        let items = table
            .iter_range(
                Bound::Included(b"key-0010".to_vec()),
                Bound::Excluded(b"key-0020".to_vec()),
            )
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(items.len(), 10);
        assert_eq!(items[0].0, b"key-0010".to_vec());
        assert_eq!(items[3], (b"key-0013".to_vec(), None));
        assert_eq!(items[9].0, b"key-0019".to_vec());

        // 反向扫描
        let items = table
            .iter_range(
                Bound::Included(b"key-0010".to_vec()),
                Bound::Excluded(b"key-0020".to_vec()),
            )
            .rev()
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(items[0].0, b"key-0019".to_vec());
        assert_eq!(items[9].0, b"key-0010".to_vec());

        // 双向交替
        let mut iter = table.iter_range(
            Bound::Included(b"key-0010".to_vec()),
            Bound::Included(b"key-0012".to_vec()),
        );
        assert_eq!(iter.next().transpose()?.unwrap().0, b"key-0010".to_vec());
        assert_eq!(
            iter.next_back().transpose()?.unwrap().0,
            b"key-0012".to_vec()
        );
        assert_eq!(iter.next().transpose()?.unwrap().0, b"key-0011".to_vec());
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
        Ok(())
    }
}
