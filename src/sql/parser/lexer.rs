use std::{fmt::Display, iter::Peekable, str::Chars};
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
}

impl Keyword {
    pub fn from_str(indent: &str) -> Option<Self> {
        Some(match indent.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
            Keyword::Bool => "Bool",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),   
    Ident(String),      
    String(String),     
    Number(String),     
    OpenParen,          //  (
    CloseParen,         //  )
    Comma,              //  ,
    Semicolon,          //  ;
    Asterisk,           //  *
    Plus,               //  +
    Minus,              //  -
    Slash,              //  /
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(string) => string,
            Token::Number(number) => number,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
        })
    }
}

pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.iter.peek().map(|c| Err(Error::Parse(format!("[Lexer] Unexpected character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable()
        }
    }
    // eg. select *      from   t;
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c|predicate(*c))?; // if none, return none
        self.iter.next()
    }

    // if token, go next and return that token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) ->Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }
        Some(value).filter(|v| !v.is_empty())
    }

    fn scan(&mut self) -> Result<Option<Token>> {
        // erase next all whitespace until there is a valid character
        self.erase_whitespace();
        // match by first character
        match self.iter.peek() {
            Some('\'') => self.scan_string(),
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),
            Some(c) if c.is_alphabetic() => Ok(self.scan_ident()),
            Some(_) => Ok(self.scan_symbol()),
            None => Ok(None),
        }
    }

    fn scan_string(&mut self) -> Result<Option<Token>> {
        // if not start by ', return, else return next
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of String"))),
            }
        }
        Ok(Some(Token::String(val)))
    }

    fn scan_number(&mut self) -> Option<Token> {
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        // check decimal point
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // check num after decimal point
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }
        Some(Token::Number(num))
    }

    // scan table/column name, true/false, Keyword
    fn scan_ident(&mut self) -> Option<Token> {
        let mut value = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            value.push(c);
        }
        Some(Keyword::from_str(&value).map_or(
            Token::Ident(value.to_lowercase()), 
            Token::Keyword
        ))
    }

    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            _ => None,
        })
    }

}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::parser::lexer::{Keyword, Token}};
    use super::Lexer;

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let sql_text = r#"
            create table tbl (
                id1 int primary key,
                id2 integer
            );
        "#;

        let tokens = Lexer::new(sql_text)
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        // println!("{:?}", tokens);
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let sql_text = r#"
            CREATE table tbl (
                id1 int primary key,
                id2 integer,
                c1 bool null,
                c2 boolean not null,
                c3 float null,
                c4 double,
                c5 string,
                c6 text,
                c7 varchar default 'foo',
                c8 int default 100,
                c9 integer
            );
        "#;

        let tokens2: Vec<Token> = Lexer::new(sql_text)
            .peekable()
            .collect::<Result<Vec<_>>>()?;
        assert!(tokens2.len() > 0);
        // println!("{:?}", tokens2);
        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1: Vec<Token> = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55)")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        // println!("{:?}", tokens1);

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
            ]
        );

        let tokens2: Vec<Token> = Lexer::new(
            "INSERT INTO       tbl (id, name, age) VALUES (100, 'db', 10);"
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;
    
        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_select() -> Result<()> {
        let tokens1: Vec<Token> = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon,
            ]
        );

        Ok(())
    }
}