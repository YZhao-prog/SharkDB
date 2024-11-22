use std::iter::Peekable;

use lexer::{Keyword, Lexer, Token};

use crate::error::{Error, Result};

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
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // check first token
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
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

    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // check table's name, must be indent type
        todo!()
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        // Option<Result<T, E>> -> Result<Option<T>, E>
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    fn next_indent(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(indent) => Ok(indent),
            token => Err(Error::Parse(format!("[Parser] Expect indent, got token {}", token))),
        }
    }
}