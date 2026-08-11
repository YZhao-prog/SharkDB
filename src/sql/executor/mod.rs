use std::sync::atomic::{AtomicUsize, Ordering};

use super::{engine::Transaction, plan::Node, types::Row};
use crate::error::Result;

mod aggregate;
pub mod expression;
mod mutation;
mod query;

// 执行结果集
#[derive(Debug)]
pub enum ResultSet {
    CreateTable { table_name: String },
    Insert { count: usize },
    Update { count: usize },
    Delete { count: usize },
    Scan { columns: Vec<String>, rows: Vec<Row> },
    Explain { plan: String },
}

// 执行入口：根据节点类型分发
pub fn execute<T: Transaction>(node: Node, txn: &mut T) -> Result<ResultSet> {
    match node {
        Node::CreateTable { schema } => {
            let table_name = schema.name.clone();
            txn.create_table(schema)?;
            Ok(ResultSet::CreateTable { table_name })
        }
        Node::Insert {
            table_name,
            columns,
            values,
        } => mutation::insert(txn, table_name, columns, values),
        Node::Update {
            table_name,
            source,
            set,
        } => mutation::update(txn, table_name, *source, set),
        Node::Delete { table_name, source } => mutation::delete(txn, table_name, *source),
        Node::Explain { source } => Ok(ResultSet::Explain {
            plan: format!("{}", source),
        }),
        // 查询类节点
        node => {
            let result = query::execute(node, txn)?;
            Ok(ResultSet::Scan {
                columns: result.columns.into_iter().map(|(_, name)| name).collect(),
                rows: result.rows,
            })
        }
    }
}

// ---------------- 并行执行配置 ----------------

// 并行度：0 表示自动（CPU 核数）
static PARALLELISM: AtomicUsize = AtomicUsize::new(0);
// 行数低于该阈值时不启用并行，避免线程开销反而拖慢小查询
pub const PARALLEL_THRESHOLD: usize = 8192;

pub fn set_parallelism(n: usize) {
    PARALLELISM.store(n, Ordering::Relaxed);
}

pub fn parallelism() -> usize {
    match PARALLELISM.load(Ordering::Relaxed) {
        0 => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
        n => n,
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::sql::engine::{kv::KVEngine, Engine};
    use crate::sql::types::{Row, Value};
    use crate::storage::memory::MemoryEngine;

    use super::ResultSet;

    fn setup() -> Result<crate::sql::engine::Session<KVEngine<MemoryEngine>>> {
        let engine = KVEngine::new(MemoryEngine::new());
        let mut s = engine.session()?;
        s.execute("create table t (id int primary key, name text, score float, active bool);")?;
        s.execute("insert into t values (1, 'alice', 90.0, true);")?;
        s.execute("insert into t values (2, 'bob', 75.5, false);")?;
        s.execute("insert into t values (3, 'carol', 82.0, true);")?;
        s.execute("insert into t values (4, 'dave', null, true);")?;
        Ok(s)
    }

    fn rows_of(result: ResultSet) -> Vec<Row> {
        match result {
            ResultSet::Scan { rows, .. } => rows,
            other => panic!("expected scan result, got {:?}", other),
        }
    }

    #[test]
    fn test_where_filter() -> Result<()> {
        let mut s = setup()?;
        // 比较 + AND
        let rows = rows_of(s.execute("select * from t where score > 80.0 and active = true;")?);
        assert_eq!(rows.len(), 2);

        // OR
        let rows = rows_of(s.execute("select * from t where id = 1 or id = 2;")?);
        assert_eq!(rows.len(), 2);

        // NULL 比较不匹配任何行
        let rows = rows_of(s.execute("select * from t where score = null;")?);
        assert_eq!(rows.len(), 0);

        // NOT
        let rows = rows_of(s.execute("select * from t where not active;")?);
        assert_eq!(rows.len(), 1);

        // 算术表达式
        let rows = rows_of(s.execute("select * from t where score * 2 > 160;")?);
        assert_eq!(rows.len(), 2);
        Ok(())
    }

    #[test]
    fn test_projection_and_alias() -> Result<()> {
        let mut s = setup()?;
        match s.execute("select name, score + 10 as boosted from t where id = 1;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["name".to_string(), "boosted".to_string()]);
                assert_eq!(
                    rows[0],
                    vec![Value::String("alice".into()), Value::Float(100.0)]
                );
            }
            other => panic!("unexpected result {:?}", other),
        }
        Ok(())
    }

    #[test]
    fn test_order_limit_offset() -> Result<()> {
        let mut s = setup()?;
        let rows = rows_of(s.execute("select id from t order by score desc limit 2;")?);
        // NULL 排最前（升序），降序排最后；90 > 82
        assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);

        let rows = rows_of(s.execute("select id from t order by id desc limit 2 offset 1;")?);
        assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(2)]]);
        Ok(())
    }

    #[test]
    fn test_update_delete() -> Result<()> {
        let mut s = setup()?;
        // 引用原值的更新
        match s.execute("update t set score = score + 5 where active = true;")? {
            ResultSet::Update { count } => assert_eq!(count, 3),
            other => panic!("unexpected result {:?}", other),
        }
        let rows = rows_of(s.execute("select score from t where id = 1;")?);
        assert_eq!(rows[0][0], Value::Float(95.0));
        // NULL + 5 仍是 NULL
        let rows = rows_of(s.execute("select score from t where id = 4;")?);
        assert_eq!(rows[0][0], Value::Null);

        // 修改主键
        s.execute("update t set id = 10 where id = 1;")?;
        assert_eq!(rows_of(s.execute("select * from t where id = 1;")?).len(), 0);
        assert_eq!(rows_of(s.execute("select * from t where id = 10;")?).len(), 1);
        // 主键冲突报错
        assert!(s.execute("update t set id = 2 where id = 3;").is_err());

        // 删除
        match s.execute("delete from t where score < 80;")? {
            ResultSet::Delete { count } => assert_eq!(count, 1),
            other => panic!("unexpected result {:?}", other),
        }
        assert_eq!(rows_of(s.execute("select * from t;")?).len(), 3);
        Ok(())
    }

    #[test]
    fn test_aggregate() -> Result<()> {
        let mut s = setup()?;
        // 无分组聚合，NULL 不参与 sum/avg/min/max，count(*) 计全部行
        let rows = rows_of(s.execute(
            "select count(*), count(score), sum(score), avg(score), min(score), max(score) from t;",
        )?);
        assert_eq!(
            rows[0],
            vec![
                Value::Integer(4),
                Value::Integer(3),
                Value::Float(247.5),
                Value::Float(82.5),
                Value::Float(75.5),
                Value::Float(90.0),
            ]
        );

        // 空表聚合输出一行
        s.execute("create table empty (a int primary key);")?;
        let rows = rows_of(s.execute("select count(*), sum(a) from empty;")?);
        assert_eq!(rows[0], vec![Value::Integer(0), Value::Null]);

        // GROUP BY（输出按分组键排序：false < true）
        let rows =
            rows_of(s.execute("select active, count(*) as cnt from t group by active;")?);
        assert_eq!(
            rows,
            vec![
                vec![Value::Boolean(false), Value::Integer(1)],
                vec![Value::Boolean(true), Value::Integer(3)],
            ]
        );

        // 非聚合列必须出现在 GROUP BY 中
        assert!(s.execute("select name, count(*) from t group by active;").is_err());
        Ok(())
    }

    #[test]
    fn test_join() -> Result<()> {
        let mut s = setup()?;
        s.execute("create table dept (id int primary key, owner int);")?;
        s.execute("insert into dept values (100, 1), (200, 3), (300, 99);")?;

        // 等值连接（优化器选择 hash join），限定列名
        let rows = rows_of(
            s.execute("select t.name, dept.id from t join dept on t.id = dept.owner;")?,
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![Value::String("alice".into()), Value::Integer(100)]));
        assert!(rows.contains(&vec![Value::String("carol".into()), Value::Integer(200)]));

        // 带附加条件的连接
        let rows = rows_of(s.execute(
            "select t.name from t join dept on t.id = dept.owner and dept.id > 100;",
        )?);
        assert_eq!(rows, vec![vec![Value::String("carol".into())]]);

        // cross join
        let rows = rows_of(s.execute("select * from t cross join dept;")?);
        assert_eq!(rows.len(), 12);
        Ok(())
    }

    #[test]
    fn test_explain() -> Result<()> {
        let mut s = setup()?;
        // 主键点查
        match s.execute("explain select * from t where id = 1;")? {
            ResultSet::Explain { plan } => assert!(plan.contains("PointLookup"), "{}", plan),
            other => panic!("unexpected result {:?}", other),
        }
        // 谓词下推进 Scan
        match s.execute("explain select * from t where score > 80;")? {
            ResultSet::Explain { plan } => {
                assert!(plan.contains("Scan: t (score > 80)"), "{}", plan)
            }
            other => panic!("unexpected result {:?}", other),
        }
        // hash join + 跨表条件下推
        s.execute("create table d (id int primary key, owner int);")?;
        match s.execute(
            "explain select * from t join d on t.id = d.owner where t.score > 80 and d.id = 5;",
        )? {
            ResultSet::Explain { plan } => {
                assert!(plan.contains("HashJoin"), "{}", plan);
                assert!(plan.contains("Scan: t (t.score > 80)"), "{}", plan);
                assert!(plan.contains("PointLookup: d"), "{}", plan);
            }
            other => panic!("unexpected result {:?}", other),
        }
        // 常量折叠
        match s.execute("explain select * from t where score > 10 * 8;")? {
            ResultSet::Explain { plan } => {
                assert!(plan.contains("score > 80"), "{}", plan)
            }
            other => panic!("unexpected result {:?}", other),
        }
        Ok(())
    }

    #[test]
    fn test_parallel_consistency() -> Result<()> {
        // 超过并行阈值的数据量下，单线程与多线程结果必须一致
        let engine = KVEngine::new(MemoryEngine::new());
        let mut s = engine.session()?;
        s.execute("create table big (id int primary key, grp int, val float);")?;
        for batch in 0..100 {
            let values = (0..100)
                .map(|i| {
                    let id = batch * 100 + i;
                    format!("({}, {}, {}.5)", id, id % 7, id % 1000)
                })
                .collect::<Vec<_>>()
                .join(", ");
            s.execute(&format!("insert into big values {};", values))?;
        }

        let query = "select grp, count(*) as cnt, sum(val) as total from big
                     where val > 100.0 group by grp order by grp;";
        super::set_parallelism(1);
        let sequential = rows_of(s.execute(query)?);
        super::set_parallelism(4);
        let parallel = rows_of(s.execute(query)?);
        super::set_parallelism(0);

        assert_eq!(sequential.len(), 7);
        assert_eq!(sequential, parallel);
        Ok(())
    }
}
