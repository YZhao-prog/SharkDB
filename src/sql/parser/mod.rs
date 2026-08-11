use std::iter::Peekable;

use ast::Column;
use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

use super::types::DataType;

pub mod ast;
mod lexer;

// 解析器定义
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // 解析，获取到抽象语法树
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        // 期望 sql 语句的最后有个分号
        self.next_expect(Token::Semicolon)?;
        // 分号之后不能有其他的符号
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // 查看第一个 Token 类型
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 解析 DDL 类型
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    // 解析 Explain 语句
    fn parse_explain(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Explain))?;
        if let Some(Token::Keyword(Keyword::Explain)) = self.peek()? {
            return Err(Error::Parse("[Parser] Cannot nest explain".into()));
        }
        Ok(ast::Statement::Explain(Box::new(self.parse_statement()?)))
    }

    // 解析 Select 语句
    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;

        // 选择列表：* 或者 expr [AS alias] 列表
        let mut select = Vec::new();
        if self.next_if_token(Token::Asterisk).is_some() {
            select.push((ast::Expression::All, None));
        } else {
            loop {
                let expr = self.parse_expression()?;
                let alias = match self.next_if_token(Token::Keyword(Keyword::As)) {
                    Some(_) => Some(self.next_ident()?),
                    None => None,
                };
                select.push((expr, alias));
                if self.next_if_token(Token::Comma).is_none() {
                    break;
                }
            }
        }

        self.next_expect(Token::Keyword(Keyword::From))?;
        let from = self.parse_from_item()?;

        // WHERE 子句
        let r#where = match self.next_if_token(Token::Keyword(Keyword::Where)) {
            Some(_) => Some(self.parse_expression()?),
            None => None,
        };

        // GROUP BY 子句
        let mut group_by = Vec::new();
        if self.next_if_token(Token::Keyword(Keyword::Group)).is_some() {
            self.next_expect(Token::Keyword(Keyword::By))?;
            loop {
                group_by.push(self.parse_expression()?);
                if self.next_if_token(Token::Comma).is_none() {
                    break;
                }
            }
        }

        // ORDER BY 子句
        let mut order_by = Vec::new();
        if self.next_if_token(Token::Keyword(Keyword::Order)).is_some() {
            self.next_expect(Token::Keyword(Keyword::By))?;
            loop {
                let expr = self.parse_expression()?;
                let direction = if self.next_if_token(Token::Keyword(Keyword::Asc)).is_some() {
                    ast::OrderDirection::Asc
                } else if self.next_if_token(Token::Keyword(Keyword::Desc)).is_some() {
                    ast::OrderDirection::Desc
                } else {
                    ast::OrderDirection::Asc
                };
                order_by.push((expr, direction));
                if self.next_if_token(Token::Comma).is_none() {
                    break;
                }
            }
        }

        // LIMIT / OFFSET 子句，两种顺序都接受
        let mut limit = None;
        let mut offset = None;
        loop {
            if limit.is_none() && self.next_if_token(Token::Keyword(Keyword::Limit)).is_some() {
                limit = Some(self.parse_expression()?);
            } else if offset.is_none()
                && self.next_if_token(Token::Keyword(Keyword::Offset)).is_some()
            {
                offset = Some(self.parse_expression()?);
            } else {
                break;
            }
        }

        Ok(ast::Statement::Select {
            select,
            from,
            r#where,
            group_by,
            order_by,
            limit,
            offset,
        })
    }

    // 解析 FROM 子句：单表或 JOIN 链
    fn parse_from_item(&mut self) -> Result<ast::FromItem> {
        let mut item = ast::FromItem::Table {
            name: self.next_ident()?,
        };
        loop {
            // CROSS JOIN
            if self.next_if_token(Token::Keyword(Keyword::Cross)).is_some() {
                self.next_expect(Token::Keyword(Keyword::Join))?;
                item = ast::FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(ast::FromItem::Table {
                        name: self.next_ident()?,
                    }),
                    join_type: ast::JoinType::Cross,
                    predicate: None,
                };
                continue;
            }
            // [INNER] JOIN ... [ON expr]
            let inner = self.next_if_token(Token::Keyword(Keyword::Inner)).is_some();
            if inner || matches!(self.peek()?, Some(Token::Keyword(Keyword::Join))) {
                self.next_expect(Token::Keyword(Keyword::Join))?;
                let right = ast::FromItem::Table {
                    name: self.next_ident()?,
                };
                let predicate = match self.next_if_token(Token::Keyword(Keyword::On)) {
                    Some(_) => Some(self.parse_expression()?),
                    None => None,
                };
                item = ast::FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(right),
                    join_type: ast::JoinType::Inner,
                    predicate,
                };
                continue;
            }
            break;
        }
        Ok(item)
    }

    // 解析 Update 语句
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Update))?;
        let table_name = self.next_ident()?;
        self.next_expect(Token::Keyword(Keyword::Set))?;

        let mut set = Vec::new();
        loop {
            let column = self.next_ident()?;
            self.next_expect(Token::Equal)?;
            set.push((column, self.parse_expression()?));
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        let r#where = match self.next_if_token(Token::Keyword(Keyword::Where)) {
            Some(_) => Some(self.parse_expression()?),
            None => None,
        };

        Ok(ast::Statement::Update {
            table_name,
            set,
            r#where,
        })
    }

    // 解析 Delete 语句
    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Delete))?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        let table_name = self.next_ident()?;

        let r#where = match self.next_if_token(Token::Keyword(Keyword::Where)) {
            Some(_) => Some(self.parse_expression()?),
            None => None,
        };

        Ok(ast::Statement::Delete {
            table_name,
            r#where,
        })
    }

    // 解析 Insert 语句
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        // 表名
        let table_name = self.next_ident()?;

        // 查看是否给指定的列进行 insert
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        // 解析 value 信息
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // insert into tbl(a, b, c) values (1, 2, 3),(4, 5, 6);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            values.push(exprs);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    // 解析 Create Table 语句
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // 期望是 Table 名
        let table_name = self.next_ident()?;
        // 表名之后应该是括号
        self.next_expect(Token::OpenParen)?;

        // 解析列信息
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果没有逗号，列解析完成，跳出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    // 解析列信息
    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
            primary_key: false,
        };

        // 解析列的约束：默认值、是否可空、主键
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true;
                }
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    // 解析表达式，优先级从低到高：
    // OR < AND < NOT < 比较 < 加减 < 乘除 < 一元负号 < 原子
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<ast::Expression> {
        let mut lhs = self.parse_and_expr()?;
        while self.next_if_token(Token::Keyword(Keyword::Or)).is_some() {
            let rhs = self.parse_and_expr()?;
            lhs = ast::Expression::Operation(ast::Operation::Or(Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_and_expr(&mut self) -> Result<ast::Expression> {
        let mut lhs = self.parse_not_expr()?;
        while self.next_if_token(Token::Keyword(Keyword::And)).is_some() {
            let rhs = self.parse_not_expr()?;
            lhs = ast::Expression::Operation(ast::Operation::And(Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_not_expr(&mut self) -> Result<ast::Expression> {
        if self.next_if_token(Token::Keyword(Keyword::Not)).is_some() {
            let expr = self.parse_not_expr()?;
            return Ok(ast::Expression::Operation(ast::Operation::Not(Box::new(
                expr,
            ))));
        }
        self.parse_comparison_expr()
    }

    fn parse_comparison_expr(&mut self) -> Result<ast::Expression> {
        use ast::Operation::*;
        let lhs = self.parse_additive_expr()?;
        let op = match self.peek()? {
            Some(Token::Equal) => Equal as fn(_, _) -> ast::Operation,
            Some(Token::NotEqual) => NotEqual,
            Some(Token::GreaterThan) => GreaterThan,
            Some(Token::GreaterThanOrEqual) => GreaterThanOrEqual,
            Some(Token::LessThan) => LessThan,
            Some(Token::LessThanOrEqual) => LessThanOrEqual,
            _ => return Ok(lhs),
        };
        self.next()?;
        let rhs = self.parse_additive_expr()?;
        Ok(ast::Expression::Operation(op(Box::new(lhs), Box::new(rhs))))
    }

    fn parse_additive_expr(&mut self) -> Result<ast::Expression> {
        let mut lhs = self.parse_multiplicative_expr()?;
        loop {
            let op = match self.peek()? {
                Some(Token::Plus) => ast::Operation::Add as fn(_, _) -> ast::Operation,
                Some(Token::Minus) => ast::Operation::Subtract,
                _ => break,
            };
            self.next()?;
            let rhs = self.parse_multiplicative_expr()?;
            lhs = ast::Expression::Operation(op(Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<ast::Expression> {
        let mut lhs = self.parse_unary_expr()?;
        loop {
            let op = match self.peek()? {
                Some(Token::Asterisk) => ast::Operation::Multiply as fn(_, _) -> ast::Operation,
                Some(Token::Slash) => ast::Operation::Divide,
                _ => break,
            };
            self.next()?;
            let rhs = self.parse_unary_expr()?;
            lhs = ast::Expression::Operation(op(Box::new(lhs), Box::new(rhs)));
        }
        Ok(lhs)
    }

    fn parse_unary_expr(&mut self) -> Result<ast::Expression> {
        if self.next_if_token(Token::Minus).is_some() {
            // 负号后面直接跟数字字面量时折叠成常量
            if let Some(Token::Number(_)) = self.peek()? {
                return Ok(match self.parse_primary_expr()? {
                    ast::Expression::Consts(ast::Consts::Integer(i)) => {
                        ast::Consts::Integer(-i).into()
                    }
                    ast::Expression::Consts(ast::Consts::Float(f)) => ast::Consts::Float(-f).into(),
                    expr => expr,
                });
            }
            let expr = self.parse_unary_expr()?;
            return Ok(ast::Expression::Operation(ast::Operation::Negate(
                Box::new(expr),
            )));
        }
        self.parse_primary_expr()
    }

    fn parse_primary_expr(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // 整数
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    // 浮点数
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            Token::OpenParen => {
                let expr = self.parse_expression()?;
                self.next_expect(Token::CloseParen)?;
                expr
            }
            Token::Ident(ident) => {
                // 函数调用：ident(expr) 或 ident(*)
                if self.next_if_token(Token::OpenParen).is_some() {
                    let arg = if self.next_if_token(Token::Asterisk).is_some() {
                        ast::Expression::All
                    } else {
                        self.parse_expression()?
                    };
                    self.next_expect(Token::CloseParen)?;
                    ast::Expression::Function(ident, Box::new(arg))
                } else if self.next_if_token(Token::Period).is_some() {
                    // 限定列名 t.a
                    ast::Expression::Field(Some(ident), self.next_ident()?)
                } else {
                    ast::Expression::Field(None, ident)
                }
            }
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            ))),
        }
    }

    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    // 如果满足条件，则跳转到下一个 Token
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    // 如果下一个 Token 是关键字，则跳转
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ast::{self, Consts, Expression, Operation},
        Parser,
    };
    use crate::error::Result;

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql = "
            create table tbl1 (
                a int primary key,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt = Parser::new(sql).parse()?;
        match stmt {
            ast::Statement::CreateTable { name, columns } => {
                assert_eq!(name, "tbl1");
                assert_eq!(columns.len(), 4);
                assert!(columns[0].primary_key);
                assert!(!columns[1].primary_key);
                assert_eq!(columns[1].nullable, Some(false));
                assert_eq!(
                    columns[3].default,
                    Some(Expression::Consts(Consts::Boolean(true)))
                );
            }
            _ => panic!("unexpected statement"),
        }

        let sql2 = "create tabl tbl1 (a int, b float);";
        assert!(Parser::new(sql2).parse().is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let stmt = Parser::new("insert into tbl1 values (1, 2, 3, 'a', true), (-4, 5.2, null, 'b', false);").parse()?;
        match stmt {
            ast::Statement::Insert {
                table_name, values, ..
            } => {
                assert_eq!(table_name, "tbl1");
                assert_eq!(values.len(), 2);
                assert_eq!(values[1][0], Expression::Consts(Consts::Integer(-4)));
            }
            _ => panic!("unexpected statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let stmt = Parser::new(
            "select a, b as bb, sum(c) from t1 join t2 on t1.a = t2.a
             where a > 10 and b < 5.5 or not c
             group by a, b order by a desc, b limit 10 offset 20;",
        )
        .parse()?;
        match stmt {
            ast::Statement::Select {
                select,
                from,
                r#where,
                group_by,
                order_by,
                limit,
                offset,
            } => {
                assert_eq!(select.len(), 3);
                assert_eq!(select[0].0, Expression::Field(None, "a".into()));
                assert_eq!(select[1].1, Some("bb".into()));
                assert!(matches!(select[2].0, Expression::Function(_, _)));
                match from {
                    ast::FromItem::Join {
                        join_type,
                        predicate,
                        ..
                    } => {
                        assert_eq!(join_type, ast::JoinType::Inner);
                        assert_eq!(
                            predicate,
                            Some(Expression::Operation(Operation::Equal(
                                Box::new(Expression::Field(Some("t1".into()), "a".into())),
                                Box::new(Expression::Field(Some("t2".into()), "a".into())),
                            )))
                        );
                    }
                    _ => panic!("expected join"),
                }
                assert!(r#where.is_some());
                assert_eq!(group_by.len(), 2);
                assert_eq!(order_by.len(), 2);
                assert_eq!(order_by[0].1, ast::OrderDirection::Desc);
                assert_eq!(order_by[1].1, ast::OrderDirection::Asc);
                assert_eq!(limit, Some(Expression::Consts(Consts::Integer(10))));
                assert_eq!(offset, Some(Expression::Consts(Consts::Integer(20))));
            }
            _ => panic!("unexpected statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parser_expression_precedence() -> Result<()> {
        // 1 + 2 * 3 应该解析成 1 + (2 * 3)
        let stmt = Parser::new("select 1 + 2 * 3 from t;").parse()?;
        match stmt {
            ast::Statement::Select { select, .. } => {
                assert_eq!(
                    select[0].0,
                    Expression::Operation(Operation::Add(
                        Box::new(Expression::Consts(Consts::Integer(1))),
                        Box::new(Expression::Operation(Operation::Multiply(
                            Box::new(Expression::Consts(Consts::Integer(2))),
                            Box::new(Expression::Consts(Consts::Integer(3))),
                        ))),
                    ))
                );
            }
            _ => panic!("unexpected statement"),
        }

        // (1 + 2) * 3 括号优先
        let stmt = Parser::new("select (1 + 2) * 3 from t;").parse()?;
        match stmt {
            ast::Statement::Select { select, .. } => {
                assert!(matches!(
                    select[0].0,
                    Expression::Operation(Operation::Multiply(_, _))
                ));
            }
            _ => panic!("unexpected statement"),
        }
        Ok(())
    }

    #[test]
    fn test_parser_update_delete() -> Result<()> {
        let stmt = Parser::new("update t set a = a + 1, b = 'x' where a = 3;").parse()?;
        match stmt {
            ast::Statement::Update {
                table_name,
                set,
                r#where,
            } => {
                assert_eq!(table_name, "t");
                assert_eq!(set.len(), 2);
                assert_eq!(set[0].0, "a");
                assert!(r#where.is_some());
            }
            _ => panic!("unexpected statement"),
        }

        let stmt = Parser::new("delete from t where a > 5;").parse()?;
        assert!(matches!(stmt, ast::Statement::Delete { .. }));

        let stmt = Parser::new("explain select * from t;").parse()?;
        assert!(matches!(stmt, ast::Statement::Explain(_)));
        Ok(())
    }
}
