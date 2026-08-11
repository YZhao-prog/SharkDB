// 零依赖的基准测试，运行方式：cargo bench
//
// 覆盖三个层次：
// 1. 存储引擎层（MemoryEngine / DiskEngine）：点写、点读、前缀扫描
// 2. MVCC 事务层：写事务（begin + set + commit）、快照读
// 3. SQL 端到端：INSERT 语句、SELECT 全表扫描
//
// 每个用例先预热，再取多轮中的中位数，输出 ops/sec 和单次操作耗时

use std::time::Instant;

use sharkdb::{
    error::Result,
    sql::engine::kv::KVEngine,
    sql::engine::Engine as SqlEngine,
    storage::{disk::DiskEngine, engine::Engine, memory::MemoryEngine, mvcc::Mvcc},
};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;
const ROUNDS: usize = 5;

fn main() -> Result<()> {
    println!("sqldb-rs benchmark ({} rounds, median reported)", ROUNDS);
    println!("{:-<72}", "");
    println!(
        "{:<32} {:>12} {:>12} {:>10}",
        "case", "ops", "ops/sec", "ns/op"
    );
    println!("{:-<72}", "");

    bench_storage_memory()?;
    bench_storage_disk()?;
    bench_mvcc()?;
    bench_sql()?;

    println!("{:-<72}", "");
    Ok(())
}

// ---------------- 存储引擎层 ----------------

fn bench_storage_memory() -> Result<()> {
    let n = 100_000;

    report("memory/set", n, || {
        let mut eng = MemoryEngine::new();
        let mut rng = Rng::new(42);
        run(|| {
            for _ in 0..n {
                eng.set(rng.key(), rng.value())?;
            }
            Ok(())
        })
    })?;

    let mut eng = MemoryEngine::new();
    fill(&mut eng, n)?;
    report("memory/get (point read)", n, || {
        let mut rng = Rng::new(42);
        run(|| {
            for _ in 0..n {
                let (k, _) = (rng.key(), rng.value());
                std::hint::black_box(eng.get(k)?);
            }
            Ok(())
        })
    })?;

    let scans = 10_000;
    report("memory/scan_prefix", scans, || {
        let mut rng = Rng::new(7);
        run(|| {
            for _ in 0..scans {
                let mut prefix = rng.key();
                prefix.truncate(2);
                let iter = eng.scan_prefix(prefix);
                std::hint::black_box(iter.count());
            }
            Ok(())
        })
    })?;

    Ok(())
}

fn bench_storage_disk() -> Result<()> {
    let n = 100_000;

    report("disk/set (append log)", n, || {
        let dir = tempfile::tempdir()?;
        let mut eng = DiskEngine::new(dir.path().join("db.log"))?;
        let mut rng = Rng::new(42);
        run(|| {
            for _ in 0..n {
                eng.set(rng.key(), rng.value())?;
            }
            Ok(())
        })
    })?;

    let dir = tempfile::tempdir()?;
    let mut eng = DiskEngine::new(dir.path().join("db.log"))?;
    fill(&mut eng, n)?;
    report("disk/get (point read)", n, || {
        let mut rng = Rng::new(42);
        run(|| {
            for _ in 0..n {
                let (k, _) = (rng.key(), rng.value());
                std::hint::black_box(eng.get(k)?);
            }
            Ok(())
        })
    })?;

    // 重启恢复：重放日志重建 keydir
    drop(eng);
    report("disk/recover (100k entries)", 1, || {
        run(|| {
            let eng = DiskEngine::new(dir.path().join("db.log"))?;
            std::hint::black_box(&eng);
            Ok(())
        })
    })?;

    Ok(())
}

// ---------------- MVCC 事务层 ----------------

fn bench_mvcc() -> Result<()> {
    let n = 10_000;

    report("mvcc/txn write (begin+set+commit)", n, || {
        let mvcc = Mvcc::new(MemoryEngine::new());
        let mut rng = Rng::new(42);
        run(|| {
            for _ in 0..n {
                let txn = mvcc.begin()?;
                txn.set(rng.key(), rng.value())?;
                txn.commit()?;
            }
            Ok(())
        })
    })?;

    let mvcc = Mvcc::new(MemoryEngine::new());
    {
        let mut rng = Rng::new(42);
        let txn = mvcc.begin()?;
        for _ in 0..n {
            txn.set(rng.key(), rng.value())?;
        }
        txn.commit()?;
    }
    report("mvcc/snapshot read", n, || {
        let mut rng = Rng::new(42);
        let txn = mvcc.begin()?;
        let r = run(|| {
            for _ in 0..n {
                let (k, _) = (rng.key(), rng.value());
                std::hint::black_box(txn.get(k)?);
            }
            Ok(())
        });
        txn.commit()?;
        r
    })?;

    Ok(())
}

// ---------------- SQL 端到端 ----------------

fn bench_sql() -> Result<()> {
    let n = 10_000;

    report("sql/insert (per statement)", n, || {
        let eng = KVEngine::new(MemoryEngine::new());
        let mut s = eng.session()?;
        s.execute("create table t (id int, name text, score float);")?;
        run(|| {
            for i in 0..n {
                s.execute(&format!(
                    "insert into t values({}, 'name-{}', {}.5);",
                    i, i, i
                ))?;
            }
            Ok(())
        })
    })?;

    let eng = KVEngine::new(MemoryEngine::new());
    let mut s = eng.session()?;
    s.execute("create table t (id int, name text, score float);")?;
    for i in 0..n {
        s.execute(&format!(
            "insert into t values({}, 'name-{}', {}.5);",
            i, i, i
        ))?;
    }
    let scans = 20;
    report("sql/select * (10k-row scan)", scans, || {
        run(|| {
            for _ in 0..scans {
                std::hint::black_box(s.execute("select * from t;")?);
            }
            Ok(())
        })
    })?;

    Ok(())
}

// ---------------- 基准测试工具 ----------------

// 计时一次闭包执行
fn run(mut f: impl FnMut() -> Result<()>) -> Result<f64> {
    let start = Instant::now();
    f()?;
    Ok(start.elapsed().as_secs_f64())
}

// 执行 ROUNDS 轮（外加一轮预热），取中位数，打印一行结果
// setup 每轮重建自己的状态，返回该轮耗时
fn report(name: &str, ops: usize, mut round: impl FnMut() -> Result<f64>) -> Result<()> {
    round()?; // 预热
    let mut times: Vec<f64> = (0..ROUNDS).map(|_| round()).collect::<Result<_>>()?;
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times[times.len() / 2];

    let ops_per_sec = ops as f64 / median;
    let ns_per_op = median * 1e9 / ops as f64;
    println!(
        "{:<32} {:>12} {:>12.0} {:>10.0}",
        name, ops, ops_per_sec, ns_per_op
    );
    Ok(())
}

// 确定性伪随机数（SplitMix64），保证 set/get 用相同种子能生成相同 key 序列
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn key(&mut self) -> Vec<u8> {
        let mut key = vec![0u8; KEY_SIZE];
        for chunk in key.chunks_mut(8) {
            let bytes = self.next().to_be_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        key
    }

    fn value(&mut self) -> Vec<u8> {
        let mut val = vec![0u8; VALUE_SIZE];
        for chunk in val.chunks_mut(8) {
            let bytes = self.next().to_be_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        val
    }
}

// 预填充数据（与 get 基准使用相同的种子 42）
fn fill(eng: &mut impl Engine, n: usize) -> Result<()> {
    let mut rng = Rng::new(42);
    for _ in 0..n {
        eng.set(rng.key(), rng.value())?;
    }
    Ok(())
}
