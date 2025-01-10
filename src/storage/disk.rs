use std::{
    collections::{btree_map, BTreeMap},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use fs4::FileExt;

use crate::error::Result;

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
const LOG_HEADER_SIZE: u32 = 8; //key len (u32=>4) + value len (u32=>4) = 8
pub struct DiskEngine {
    keydir: KeyDir, // memory index:  BTreeMap<Vec<u8>, (u64, u32)>: key->(offset, value len)
    log: Log,
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        let mut log = Log::new(file_path)?;
        // boot, recover keydir
        let keydir = log.build_keydir()?;
        Ok(Self { keydir, log })
    }

    // rewrite log data to a new tmp file, then set tmp file as formal data file
    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    // when we delete or set new value to a key, we will update keydir and append info to log
    // what we need to do here is to rewrite log by keydir
    fn compact(&mut self) -> Result<()> {
        // create new file with suffix "compact"
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");
        let mut new_log = Log::new(new_path)?;
        let mut new_keydir: BTreeMap<Vec<u8>, (u64, u32)> = KeyDir::new();
        // rewrite
        for (key, (offset, value_size)) in self.keydir.iter() {
            // get value
            let value = self.log.read_value(*offset, *value_size)?;
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(
                key.clone(),
                (
                    new_offset + new_size as u64 - *value_size as u64,
                    *value_size,
                ),
            );
        }
        // replace tmp file as formal file
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;
        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;
        Ok(())
    }
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator<'a>;
    // +----------------+------------------+--------------------+---------------------+
    // | Key Length (4) | Value Length (4) | Key (Variable)     | Value (Variable)    |
    // +----------------+------------------+--------------------+---------------------+
    // append log to disk, get (offset, value len)
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // wirte to disk
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // update memory index
        // 这里offset具体用途：当一条记录写入完成后，文件的下一个空闲位置就是 offset + size，这是新记录的写入起点。
        // eg: offset = 100, size = 50  =>  100---------|----150
        // value len = 20                              130
        // key len, value len, key => 100---130   value => 130---150
        let value_size = value.len() as u32;
        // insert key | (offset of value, value len) => (130, 20)      这里offset含义：日志记录中 Value 数据的起始位置
        self.keydir
            .insert(key, (offset + size as u64 - value_size as u64, value_size));
        Ok(())
    }

    // get data in disk by (offset of value, value len) in keydir
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, value_size)) => {
                let val = self.log.read_value(*offset, *value_size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        DiskEngineIterator {
            inner: self.keydir.range(range),
            log: &mut self.log
        }
    }
}

pub struct DiskEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (offset, value_size)) = item;
        let value = self.log.read_value(*offset, *value_size)?;
        // •	key.clone()：这里调用 clone 是因为 key 是一个引用类型（&Vec<u8>），而我们需要返回一个拥有所有权的 Vec<u8>，所以需要克隆。
        // •	value：因为 read_value 返回的 value 已经是一个拥有所有权的值，因此可以直接返回。
        Ok((key.clone(), value))
    }
}

impl<'a> super::engine::EngineIterator for DiskEngineIterator<'a> {}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

// A file
struct Log {
    file_path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(file_path: PathBuf) -> Result<Self> {
        // check dir exist, if not exist, create recursively by using create_dir_all()
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }
        // open
        let file = OpenOptions::new()
            .create(true) // create the file if it does not exist
            .read(true)
            .write(true)
            .open(&file_path)?;

        // add exclusive lock, ensure only one service use this file
        file.try_lock_exclusive()?;

        Ok(Self { file_path, file })
    }
}

impl Log {
    // +----------------+------------------+--------------------+---------------------+
    // | Key Length (4) | Value Length (4) | Key (Variable)     | Value (Variable)    |
    // +----------------+------------------+--------------------+---------------------+
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // move to the tail of the file, and append data
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let value_size = value.map_or(0, |v: &Vec<u8>| v.len() as u32);
        let total_size = LOG_HEADER_SIZE + key_size + value_size; // key len + value len + mutable key info size + mutable value info size
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        // write to buffer => key len | value len | key | value
        writer.write_all(&key_size.to_be_bytes())?;
        // None -> -1 -> delete
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(v)?;
        }
        // flush buffer data to disk
        writer.flush()?;
        // data store in    offset ---------- offset + total size
        Ok((offset, total_size))
    }

    // traverse disk file, and get all the log, build new memory index
    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();
        let mut buf_reader = BufReader::new(&self.file);
        let file_size = self.file.metadata()?.len();

        let mut offset = 0;
        loop {
            if offset >= file_size {
                break;
            }
            let (key, value_size) = Self::read_entry(&mut buf_reader, offset)?;
            let key_size = key.len() as u32;
            if value_size == -1 {
                keydir.remove(&key);
                // No value, mutable value size = 0;
                offset += LOG_HEADER_SIZE as u64 + key_size as u64;
            } else {
                keydir.insert(
                    key,
                    (
                        offset + LOG_HEADER_SIZE as u64 + key_size as u64,
                        value_size as u32,
                    ),
                );
                offset += key_size as u64 + value_size as u64 + LOG_HEADER_SIZE as u64;
            }
        }
        Ok(keydir)
    }

    fn read_value(&mut self, offset: u64, value_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; value_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        buf_reader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4]; // 4 bytes
                                  // read key size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);
        // reaf value size, reuse buf
        buf_reader.read_exact(&mut len_buf)?;
        let value_size = i32::from_be_bytes(len_buf);
        // read key info
        let mut key = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key)?;
        Ok((key, value_size))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, engine::Engine},
    };
    use std::path::PathBuf;

    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        let mut eng = DiskEngine::new(PathBuf::from("/Users/zy/Desktop/SharkDB/tmp/SharkDB-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;
        // ➜ xxd tmp/SharkDB-log
        // 00000000: 0000 0004 0000 0005 6b65 7931 7661 6c75  ........key1valu
        // 00000010: 6500 0000 0400 0000 056b 6579 3276 616c  e........key2val
        // 00000020: 7565 0000 0004 0000 0005 6b65 7933 7661  ue........key3va
        // 00000030: 6c75 6500 0000 04ff ffff ff6b 6579 3100  lue........key1.
        // 00000040: 0000 04ff ffff ff6b 6579 32              .......key2

        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;


        let iter = eng.scan(..);
        let v: Vec<(Vec<u8>, Vec<u8>)> = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("/Users/zy/Desktop/SharkDB/tmp/SharkDB-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);
        // ➜ xxd tmp/SharkDB-log
        // 00000000: 0000 0002 0000 0006 6161 7661 6c75 6533  ........aavalue3
        // 00000010: 0000 0002 0000 0006 6262 7661 6c75 6535  ........bbvalue5
        // 00000020: 0000 0004 0000 0005 6b65 7933 7661 6c75  ........key3valu
        // 00000030: 65  

        std::fs::remove_dir_all("/Users/zy/Desktop/SharkDB/tmp/")?;

        Ok(())
    }
}
