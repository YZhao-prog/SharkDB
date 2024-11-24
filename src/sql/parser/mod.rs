use std::iter::Peekable;

use ast::Column;
use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

use super::types::DataType;

mod lexer;

pub mod ast;

pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }
    // parse and get ast tree
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        // expect find a ";" after sql
        self.next_expect(Token::Semicolon)?;
        // If there is something else after ";", then this is an illegal sql
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // check first token
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }


    // INSERT INTO employees (id, name, salary)
    // VALUES (1, 'Alice', 50000);
    //          OR
    // INSERT INTO employees
    // VALUES (1, 'Alice', 50000);
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        // check 'insert into'
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;
        // check table name
        let table_name = self.next_indent()?;
        // check "(" so we know if we have column name here, and get column info
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_indent()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    token =>  return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
                }
            }
            Some(cols)
        } else {
            None
        };
        // parse value
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // insert into tbl(a, b, c) values (1, 2, 3), (4, 5, 6);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    token =>  return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
                }
            }
            values.push(exprs);
            // if no "," afterwards, finish and break
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(ast::Statement::Insert { table_name, columns, values})
    }

    fn parse_select(&mut self) -> Result<ast::Statement> {
        // check 'select * from'
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        // check table name
        let table_name = self.next_indent()?;
        Ok(ast::Statement::Select { table_name })
    }

    // parse ddl type，create xxx, drop xxx
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        // find next of create/drop
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    // CREATE TABLE table_name (
    //     id INT NOT NULL DEFAULT 0
    //     ...
    // );
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // check table's name, must be indent type
        let table_name = self.next_indent()?;
        // check "(" afther table name
        self.next_expect(Token::OpenParen)?;
        // check column after "("
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // if no "," afterwards, finish parse column
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        // check ")"
        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable { name: table_name, columns: columns })
    }

    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_indent()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Boolean) | Token::Keyword(Keyword::Bool) => DataType::Boolean,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Text) 
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
        };
        // check if this column could have default value, and if it is nullable
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword { 
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                },
                Keyword::Default => column.default = Some(self.parse_expression()?),
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }
        Ok(column)
    }

    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // Integer
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    // Float
                    ast::Consts::Float(n.parse()?).into()
                }
            },
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => return Err(Error::Parse(format!("[Parser] Unexpected token {}", t)))
        })
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        // Option<Result<T, E>> -> Result<Option<T>, E>
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        // Some(Token) -> Token -> Ok(Token)
        // None -> Err
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    fn next_indent(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(indent) => Ok(indent),
            token => Err(Error::Parse(format!("[Parser] Expect indent, got token {}", token))),
        }
    }

    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!("[Parser] Expect token {}, got token {}",expect ,token)));
        }
        return Ok(());
    }

    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        // if none, return none in advance; if Some(Token), continue
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok() // Go next; Result -> Option
    }
    // if this is a keyword, go next and return
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}


#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::{parser::ast, types::DataType}};

    use super::Parser;

    #[test]
    fn test_parse_create_table() -> Result<()> {
        let sql = "
            Create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt = Parser::new(sql).parse()?;
        let expected_stmt = ast::Statement::CreateTable {
            name: "tbl1".to_string(),
            columns: vec![
                ast::Column {
                    name: "a".to_string(),
                    datatype: DataType::Integer,
                    nullable: None,
                    default: Some(ast::Consts::Integer(100).into()),
                },
                ast::Column {
                    name: "b".to_string(),
                    datatype: DataType::Float,
                    nullable: Some(false),
                    default: None,
                },
                ast::Column {
                    name: "c".to_string(),
                    datatype: DataType::String,
                    nullable: Some(true),
                    default: None,
                },
                ast::Column {
                    name: "d".to_string(),
                    datatype: DataType::Boolean,
                    nullable: None,
                    default: Some(ast::Consts::Boolean(true).into()),
                },
            ],
        };
        assert_eq!(stmt, expected_stmt);
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                table_name: "tbl1".to_string()
            }
        );
        Ok(())
    }
}