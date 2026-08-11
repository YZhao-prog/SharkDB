// Bloom 过滤器，用于 SSTable 点查时快速判断 key 是否可能存在，
// 避免对不包含该 key 的 SSTable 做磁盘读取
//
// 采用双重哈希（double hashing）：只需要对 key 计算一次 FNV-1a 哈希 h1，
// 再由 h1 派生出 h2，第 i 个 bit 位置为 (h1 + i * h2) % nbits

const BITS_PER_KEY: usize = 10;
const NUM_HASHES: u32 = 7;

pub struct BloomFilter {
    k: u32,
    nbits: u64,
    bits: Vec<u8>,
}

// FNV-1a 64 位哈希
pub fn hash(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// 由 h1 派生第二个哈希
fn hash2(h1: u64) -> u64 {
    // 乘以奇数常量再旋转，保证 h2 与 h1 尽量独立且不为 0
    h1.wrapping_mul(0x9E3779B97F4A7C15).rotate_left(31) | 1
}

impl BloomFilter {
    // 根据一批 key 的哈希值构建过滤器
    pub fn from_hashes(hashes: &[u64]) -> Self {
        let nbits = std::cmp::max(64, (hashes.len() * BITS_PER_KEY) as u64);
        let mut bits = vec![0u8; nbits.div_ceil(8) as usize];
        for &h1 in hashes {
            let h2 = hash2(h1);
            for i in 0..NUM_HASHES as u64 {
                let bit = h1.wrapping_add(i.wrapping_mul(h2)) % nbits;
                bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        Self {
            k: NUM_HASHES,
            nbits,
            bits,
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let h1 = hash(key);
        let h2 = hash2(h1);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.nbits;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    // 编码格式：k(4) + nbits(8) + bits
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&self.k.to_be_bytes());
        out.extend_from_slice(&self.nbits.to_be_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(data: &[u8]) -> Self {
        let k = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let nbits = u64::from_be_bytes(data[4..12].try_into().unwrap());
        Self {
            k,
            nbits,
            bits: data[12..].to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom() {
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let hashes: Vec<u64> = keys.iter().map(|k| hash(k)).collect();
        let bloom = BloomFilter::from_hashes(&hashes);

        // 已插入的 key 一定命中
        for key in &keys {
            assert!(bloom.may_contain(key));
        }

        // 未插入的 key 大部分不命中（10 bits/key 理论误判率约 1%）
        let false_positives = (1000..11000u32)
            .filter(|i| bloom.may_contain(&i.to_be_bytes()))
            .count();
        assert!(false_positives < 500, "误判过多: {}", false_positives);
    }

    #[test]
    fn test_bloom_encode_decode() {
        let hashes: Vec<u64> = (0..100u32).map(|i| hash(&i.to_be_bytes())).collect();
        let bloom = BloomFilter::from_hashes(&hashes);
        let decoded = BloomFilter::decode(&bloom.encode());
        for i in 0..100u32 {
            assert!(decoded.may_contain(&i.to_be_bytes()));
        }
    }
}
