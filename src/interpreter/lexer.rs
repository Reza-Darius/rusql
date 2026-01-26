use std::{collections::HashMap, iter::Peekable, str::Chars};

use tracing::debug;

use crate::database::errors::*;
use crate::interpreter::tokens::*;

pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    empty: bool,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        if self.empty {
            return None;
        }
        match self.next_token() {
            Token::EOF => {
                self.empty = true;
                Some(Token::EOF)
            }
            token => Some(token),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer {
            input: input.chars().peekable(),
            empty: false,
        }
    }

    fn next_token(&mut self) -> Token {
        if self.empty {
            return Token::EOF;
        }

        let iter = &mut self.input;
        let mut substring: String = String::new();
        skip_whitespace(iter);

        // check for single character token
        if let Some(t) = parse_char(iter) {
            return t;
        }

        // create substring
        while let Some(c) = self.input.next() {
            if !c.is_whitespace() {
                substring.push(c);
            } else {
                break;
            }
        }
        parse_keyword(&substring).unwrap_or_else(|| parse_value(&substring))
    }
}

fn skip_whitespace(iter: &mut Peekable<Chars<'_>>) {
    debug!("skipping whitespace");
    while let Some(c) = iter.peek() {
        if c.is_whitespace() {
            iter.next();
        } else {
            return;
        }
    }
}

fn parse_char(iter: &mut Peekable<Chars<'_>>) -> Option<Token> {
    debug!("parsing char");
    match iter.peek() {
        Some(c) => match *c {
            LPAREN => Some(Token::Seperator(Seperator::LParen)),
            RPAREN => Some(Token::Seperator(Seperator::RParen)),
            COMMA => Some(Token::Seperator(Seperator::Comma)),
            ASSIGN => Some(Token::Operator(Operator::ASSIGN)),
            PLUS => Some(Token::Operator(Operator::PLUS)),
            MINUS => Some(Token::Operator(Operator::MINUS)),
            MULTI => Some(Token::Operator(Operator::MULTI)),
            DIVIDE => Some(Token::Operator(Operator::DIVIDE)),
            MODULO => Some(Token::Operator(Operator::MODULO)),
            LT => {
                iter.next();
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::LE))
                } else {
                    Some(Token::Operator(Operator::LT))
                }
            }
            GT => {
                iter.next();
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::GE))
                } else {
                    Some(Token::Operator(Operator::GT))
                }
            }

            _ => None,
        },
        None => Some(Token::EOF),
    }
}

fn parse_keyword(string: &str) -> Option<Token> {
    KEYWORDS.with(|e| {
        if let Some(k) = e.get(string.to_ascii_lowercase().as_str()) {
            debug!(?k, "Keyword found");
            Some(Token::Keyword(*k))
        } else {
            debug!(string, "keyword not found");
            None
        }
    })
}

fn parse_value(string: &str) -> Token {
    if string.starts_with('"') && string.ends_with('"') {
        return Token::Value(Value::Str(string.trim_matches('"').to_string()));
    }
    if let Ok(int) = string.parse::<i64>() {
        Token::Value(Value::Int(int))
    } else {
        Token::Ident(string.to_string())
    }
}

#[cfg(test)]
mod lexer_test {
    use super::*;
    use test_log::test;

    #[test]
    fn token_test() -> Result<()> {
        let input = "SELECT ALL FROM my_table";
        let mut lexer = Lexer::new(input);

        let t1 = lexer.next_token();
        let t2 = lexer.next_token();
        let t3 = lexer.next_token();
        let t4 = lexer.next_token();
        let t5 = lexer.next_token();

        assert_eq!(t1, Token::Keyword(Keyword::SELECT));
        assert_eq!(t2, Token::Keyword(Keyword::ALL));
        assert_eq!(t3, Token::Keyword(Keyword::FROM));
        assert_eq!(t4, Token::Ident("my_table".to_string()));
        assert_eq!(t5, Token::EOF);
        Ok(())
    }

    #[test]
    fn token_test2() -> Result<()> {
        let input = "SELECT name FROM my_table WHERE x >= 5";
        let tokens: Vec<Token> = Lexer::new(input).collect();
        let mut iter = tokens.into_iter();

        assert_eq!(iter.next().unwrap(), Token::Keyword(Keyword::SELECT));
        assert_eq!(iter.next().unwrap(), Token::Ident("name".to_string()));
        assert_eq!(iter.next().unwrap(), Token::Keyword(Keyword::FROM));
        assert_eq!(iter.next().unwrap(), Token::Ident("my_table".to_string()));
        assert_eq!(iter.next().unwrap(), Token::Keyword(Keyword::WHERE));
        assert_eq!(iter.next().unwrap(), Token::Ident("x".to_string()));
        assert_eq!(iter.next().unwrap(), Token::Operator(Operator::GE));
        assert_eq!(iter.next().unwrap(), Token::Value(Value::Int(5i64)));
        assert_eq!(iter.next().unwrap(), Token::EOF);
        assert!(iter.next().is_none());

        Ok(())
    }
}
