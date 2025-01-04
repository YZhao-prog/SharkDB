use std::{collections::BTreeMap, io::{BufWriter, Read, Seek, SeekFrom, Write}};

use serde::de::value;

use crate::error::Result;

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;
const LOG_HEADER_SIZE: u32 = 8; //key len (u32=>4) + value len (u32=>4) = 8
pub struct DiskEngine {
    keydir: KeyDir, // BTreeMap<Vec<u8>, (u64, u32)>: key->(offset, value len)
    log: Log,
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator;
    // +----------------+------------------+--------------------+---------------------+
    // | Key Length (4) | Value Length (4) | Key (Variable)     | Value (Variable)    |
    // +----------------+------------------+--------------------+---------------------+
    // append log to disk, get (offset, value len)
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // update memory index
        // 这里offset具体用途：当一条记录写入完成后，文件的下一个空闲位置就是 offset + size，这是新记录的写入起点。
        // eg: offset = 100, size = 50  =>  100---------|----150
        // value len = 20                              130
        // key len, value len, key => 100---130   value => 130---150
        let value_size = value.len() as u32;
        // insert key | (offset of value, value len) => (130, 20)      这里offset含义：日志记录中 Value 数据的起始位置
        self.keydir.insert(key, (offset + size as u64 - value_size as u64, value_size));
        Ok(())
    }

    // get data in disk by (offset of value, value len) in keydir
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, value_size)) => {
                let val = self.log.read_value(*offset, *value_size)?;
                Ok(Some(val))
            },
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}

pub struct DiskEngineIterator {

}

impl super::engine::EngineIterator for DiskEngineIterator {

}

impl Iterator for DiskEngineIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for DiskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

// A file
struct Log {
    file: std::fs::File,
}

impl Log {
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // move to the tail of the file, and append data
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let value_size = value.map_or(0, |v: &Vec<u8>| v.len() as u32);
        let total_size =  LOG_HEADER_SIZE + key_size + value_size; // key len + value len + mutable key info size + mutable value info size
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        // write to buffer => key len | value len | key | value
        writer.write_all(&key_size.to_be_bytes())?;
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

    fn read_value(&mut self, offset: u64, value_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; value_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}
