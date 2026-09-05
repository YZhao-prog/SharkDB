# SharkDB

A SQL database engine written from scratch in Rust — hand-written parser, rule-based query optimizer, parallel execution engine, MVCC transactions with snapshot isolation, and three interchangeable storage engines including a full LSM-tree.

No parser generators, no storage libraries: the lexer, parser, planner, optimizer, executors, MVCC layer, order-preserving key codec, and all three storage engines (including WAL, SSTables, Bloom filters, and leveled compaction) are implemented by hand. Runtime dependencies are limited to `serde`/`bincode` (row serialization) and `fs4` (file locking).

```
                        SQL text
                           │
      ┌────────────────────▼────────────────────┐
      │  Parser      hand-written lexer +       │
      │              recursive-descent parser   │
      ├─────────────────────────────────────────┤
      │  Planner     AST → operator tree        │
      ├─────────────────────────────────────────┤
      │  Optimizer   constant folding,          │
      │  (rule-based) predicate pushdown,       │
      │              PK point-lookup rewrite,   │
      │              hash-join selection        │
      ├─────────────────────────────────────────┤
      │  Executor    scans, filters, joins,     │
      │              aggregates, sort/limit —   │
      │              parallel where it pays off │
      ├─────────────────────────────────────────┤
      │  MVCC        snapshot isolation,        │
      │              write-conflict detection   │
      ├─────────────────────────────────────────┤
      │  Storage     one Engine trait,          │
      │              three implementations      │
      └──┬────────────────┬────────────────┬────┘
         │                │                │
   ┌─────▼─────┐   ┌──────▼──────┐  ┌──────▼──────┐
   │  Memory   │   │   Bitcask   │  │  LSM-tree   │
   │  B-tree   │   │ append-only │  │ WAL+memtable│
   │           │   │ log + keydir│  │ SSTables    │
   └───────────┘   └─────────────┘  └─────────────┘
```

## Features

**SQL**

- DDL/DML: `CREATE TABLE` (with `PRIMARY KEY`, `NOT NULL`, `DEFAULT`), `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- Expressions with arithmetic, comparisons, `AND`/`OR`/`NOT`, and SQL ternary `NULL` semantics
- `WHERE`, `ORDER BY` (multi-key, `ASC`/`DESC`), `LIMIT`/`OFFSET`, column aliases, qualified names (`t.a`)
- Aggregates `count` / `sum` / `avg` / `min` / `max` with `GROUP BY`
- `CROSS JOIN` and `INNER JOIN ... ON`, executed as nested-loop or hash join
- `EXPLAIN` to inspect the optimized plan

**Query optimizer** (rule-based, applied bottom-up over the operator tree)

- Constant folding — `WHERE score > 10 * 8` becomes `score > 80` at plan time
- Predicate pushdown — filters merge into scans; conjuncts crossing a join are split to whichever side can evaluate them
- Primary-key point lookup — `WHERE id = 3` rewrites a full scan into an O(1) point read
- Hash-join selection — equi-join predicates upgrade nested-loop joins (O(n·m)) to hash joins (O(n+m))

```
sql> explain select * from t join d on t.id = d.owner where t.score > 80 and d.id = 5;
HashJoin: t.id = d.owner
└─ Scan: t (t.score > 80)
└─ PointLookup: d (pk = 5)
```

**Parallel execution** (`std::thread::scope`, no thread-pool dependency)

- Parallel filter: per-chunk predicate evaluation into a keep-mask, then an in-order `retain` — results are byte-identical to single-threaded execution
- Parallel two-phase aggregation: per-chunk partial aggregation with mergeable accumulators, classic partial-agg design
- Parallel scan decoding of stored rows

**gRPC server** (optional, behind the `grpc` feature)

- `Execute` returns a whole result; `ExecuteStream` sends column metadata first and then rows in batches, so a large scan is not carried in one message
- Sessions are server-side handles with an explicit lifecycle; an unknown or closed session is rejected rather than silently re-created
- Statements run on the blocking thread pool, so a long scan never stalls the async reactor
- Engine errors map onto status codes by fault: a parse error is `InvalidArgument`, a write conflict is `Aborted`, anything else is `Internal`
- Codegen uses a vendored `protoc`, so building needs no protobuf compiler on the machine

**Transactions (MVCC)**

- Snapshot isolation: each transaction sees a consistent snapshot defined by its version and the set of transactions active at begin
- Optimistic write-conflict detection (first-writer-wins); losers abort with a conflict error
- Verified against dirty reads, non-repeatable reads, and phantom reads

**Storage engines** — one `Engine` trait (`set/get/delete/scan/scan_prefix` with double-ended iterators), three implementations, all passing a shared conformance test suite:

| Engine | Design | Point read | Recovery (100K keys) |
|---|---|---:|---:|
| Memory | in-memory B-tree | 5.2M ops/s | — (volatile) |
| Bitcask | append-only log + full in-memory key index | 1.28M ops/s | 90 ms (full log replay) |
| LSM-tree | WAL + memtable, block-based SSTables, Bloom filters, leveled compaction | 480K ops/s | **5.5 ms** (~16× faster) |

The LSM engine includes: 4KB data blocks with sparse indexes, Bloom filters (10 bits/key) to skip tables on point reads, leveled compaction with tombstone GC at the bottommost level, crash recovery via WAL replay, and a bidirectional k-way merge iterator that supports interleaved forward/backward consumption without duplicates.

## Benchmarks

Zero-dependency harness (`cargo bench`), 5 rounds, median reported. Apple M-series, 100K entries (16B keys / 100B values) for storage benchmarks, 200K rows for SQL analytics.

```
case                                      ops      ops/sec      ns/op
------------------------------------------------------------------------
memory/get (point read)                100000      5208966        192
disk/get (point read)                  100000      1280440        781
disk/recover (100k entries)                 1           11   89706750
lsm/set (wal+memtable)                 100000       516816       1935
lsm/get (point read)                   100000       480359       2082
lsm/recover (100k entries)                  1          182    5506458
mvcc/txn write (begin+set+commit)        10000       624361       1602
sql/insert (per statement)              10000       183089       5462
sql/analytic 200k rows (1 thread)           10            8  125193158
sql/analytic 200k rows (N threads)          10           16    61325058
sql/analytic parallel speedup            2.04x
sql/point select (pk lookup)            10000       197997       5051
sql/point select over lsm               10000        18014       55513
```

Highlights:

- **~2× end-to-end speedup** on a 200K-row filter + multi-aggregate `GROUP BY` query from parallel execution (the residual serial fraction is the mutex-guarded MVCC scan — Amdahl in action)
- **~200K QPS** end-to-end SQL point lookups (parse → plan → optimize → execute → commit per query) on the in-memory engine; ~18K QPS on the LSM engine
- **LSM crash recovery ~16× faster than log replay**: SSTables persist their own indexes, so restart only reads index blocks plus a bounded WAL, instead of replaying the entire data log

### A war story: the LSM tombstone anti-pattern

Benchmarking SQL point lookups over the LSM engine initially showed **163 QPS** — 1000× slower than expected. The cause: MVCC's `begin` scanned the active-transaction prefix on every transaction, and each finished transaction leaves a deletion **tombstone** there. On the B-tree and Bitcask engines deletes physically remove entries, but on an LSM tombstones persist until compaction — so every `begin` skimmed thousands of dead entries (the same anti-pattern as using Cassandra as a queue). Caching the active-transaction set in memory (seeded from storage once, incrementally maintained, identical crash-recovery semantics) restored throughput to **20.7K QPS — a 127× improvement**.

## Getting started

```bash
cargo test                      # 58 tests: engine conformance, MVCC isolation, SQL end-to-end, optimizer plans
cargo test --features grpc      # + 13 gRPC integration tests over a real TCP socket
cargo bench                     # zero-dependency benchmark suite
```

Run it as a server:

```bash
cargo run --features grpc --bin sharkdb-server -- --engine lsm --data ./data
cargo run --features grpc --example grpc_client        # in another shell
```

The core engine depends on five crates. `tonic`, `prost` and `tokio` arrive only
with `--features grpc`, so a library user who does not want a network stack does
not compile one.

Use as a library:

```rust
use sharkdb::sql::engine::{kv::KVEngine, Engine};
use sharkdb::storage::lsm::LsmEngine;

let engine = KVEngine::new(LsmEngine::new("/tmp/sharkdb".into())?);
let mut session = engine.session()?;

session.execute("create table users (id int primary key, name text, score float);")?;
session.execute("insert into users values (1, 'alice', 90.0), (2, 'bob', 75.5);")?;
session.execute("update users set score = score + 5 where id = 1;")?;
let result = session.execute(
    "select name, count(*) from users where score > 70 group by name order by name;",
)?;
```

Swap `LsmEngine` for `MemoryEngine` or `DiskEngine` — the SQL and transaction layers are generic over the storage trait.

## Design notes

- **Order-preserving key codec**: a custom `serde` serializer encodes composite keys (e.g. `(table, primary_key)`, `(key, version)`) so that lexicographic byte order matches logical order — `0x00` is escaped as `0x00 0xFF` with a `0x00 0x00` terminator — letting MVCC version chains and table rows be range-scanned directly on a flat KV store. The scan hot path uses a hand-rolled decoder instead of the serde machinery.
- **MVCC on a plain KV store**: versions are `(key, version)` entries; a transaction's snapshot is `{version, active set at begin}`; a write conflicts iff a version newer than the snapshot exists on that key. Rollback replays the transaction's write-set records.
- **Deterministic parallelism**: parallel filter and aggregation are engineered to produce results identical to single-threaded execution (ordered retain-masks; sorted group output), so correctness is testable by direct comparison.

Known limitations (deliberately scoped): no `HAVING`, no outer joins, no subqueries, no secondary indexes, LIMIT/OFFSET must be constants, MVCC old versions are not yet garbage-collected, and compaction is not tombstone-ratio-triggered. See the roadmap.

## Roadmap

- Secondary indexes and index selection in the optimizer
- A CLI REPL over the gRPC endpoint
- MVCC old-version GC below the oldest active snapshot watermark
- Tombstone-ratio-triggered compaction
- Outer joins, `HAVING`, subqueries

## Acknowledgments

The initial storage/SQL skeleton (Bitcask-style engine, MVCC basics, parser foundations) follows [roseduan's build-a-database-from-scratch tutorial](https://github.com/roseduan/sqldb-rs). The LSM-tree engine, full SQL layer (expressions, mutations, aggregates, joins), rule-based optimizer, parallel execution engine, benchmark harness, and the bug fixes and performance work documented above are original work built on top of it.

## License

MIT
