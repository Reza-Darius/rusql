use std::{collections::HashMap, iter::Peekable, str::Chars};

use tracing::debug;

use crate::database::errors::*;
use crate::interpreter::tokens::*;

pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    pub current: Token,
    pub next: Token,
    empty: bool,
    first: bool,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer {
            input: input.chars().peekable(),
            current: Token::EOF,
            next: Token::EOF,
            empty: false,
            first: true,
        }
    }

    /// advances iterator, returns current token
    pub fn next(&mut self) -> Option<&Token> {
        let t = self.next_token();

        if self.first {
            self.current = t;
            self.next = self.next_token();
            self.first = false;
        } else {
            self.current = std::mem::take(&mut self.next);
            self.next = t;
        }

        debug!(?self.current, "current token");
        debug!(?self.next, "next token");

        match &self.current {
            Token::EOF => None,
            token => Some(token),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn peek(&self) -> Option<&Token> {
        match &self.next {
            Token::EOF => None,
            token => Some(token),
        }
    }

    fn next_token(&mut self) -> Token {
        if self.empty {
            // debug!("returning EOF Token");
            return Token::EOF;
        }

        let iter = &mut self.input;
        let mut substring: String = String::new();
        skip_whitespace(iter);

        // check for single character token
        if let Some(t) = parse_char(iter) {
            // debug!(?t, "returning token");
            iter.next();
            return t;
        }

        // create substring
        while let Some(c) = iter.peek() {
            if !c.is_whitespace() && !is_operator(*c) {
                // debug!(%c, "pushing char");
                substring.push(*c);
                iter.next();
            } else {
                break;
            }
        }
        let t = parse_keyword(&substring).unwrap_or_else(|| parse_value(&substring));
        // debug!(?t, "returning token");
        // debug!(char = ?iter.peek(), "next char");
        t
    }
}

fn skip_whitespace(iter: &mut Peekable<Chars<'_>>) {
    // debug!("skipping whitespace");
    while let Some(c) = iter.peek() {
        if c.is_whitespace() {
            iter.next();
        } else {
            return;
        }
    }
}

fn parse_char(iter: &mut Peekable<Chars<'_>>) -> Option<Token> {
    // debug!("parsing char");
    match iter.peek() {
        Some(c) => match *c {
            LPAREN => Some(Token::Seperator(Seperator::LParen)),
            RPAREN => Some(Token::Seperator(Seperator::RParen)),
            COMMA => Some(Token::Seperator(Seperator::Comma)),
            ASSIGN => {
                iter.next();
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::EQUAL))
                } else {
                    Some(Token::Operator(Operator::ASSIGN))
                }
            }
            PLUS => Some(Token::Operator(Operator::PLUS)),
            MINUS => Some(Token::Operator(Operator::MINUS)),
            MULTI => Some(Token::Operator(Operator::MULTI)),
            DIVIDE => Some(Token::Operator(Operator::DIVIDE)),
            MODULO => Some(Token::Operator(Operator::MODULO)),
            LT => {
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

fn is_operator(ch: char) -> bool {
    // debug!(%ch,"peeking char");
    match ch {
        LPAREN => true,
        RPAREN => true,
        COMMA => true,
        ASSIGN => true,
        PLUS => true,
        MINUS => true,
        MULTI => true,
        DIVIDE => true,
        MODULO => true,
        LT => true,
        GT => true,

        _ => false,
    }
}

fn parse_keyword(string: &str) -> Option<Token> {
    KEYWORDS.with(|e| {
        if let Some(k) = e.get(string.to_ascii_lowercase().as_str()) {
            // debug!("Keyword found");
            Some(Token::Keyword(*k))
        } else {
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
        let input = "SELECT name FROM my_table WHERE (x >= 5)";
        let mut tokens = Lexer::new(input);

        tokens.next();
        assert_eq!(tokens.current, Token::Keyword(Keyword::SELECT));
        tokens.next();
        assert_eq!(tokens.current, Token::Ident("name".to_string()));
        tokens.next();
        assert_eq!(tokens.current, Token::Keyword(Keyword::FROM));
        tokens.next();
        assert_eq!(tokens.current, Token::Ident("my_table".to_string()));
        tokens.next();
        assert_eq!(tokens.current, Token::Keyword(Keyword::WHERE));
        tokens.next();
        assert_eq!(tokens.current, Token::Seperator(Seperator::LParen));
        tokens.next();
        assert_eq!(tokens.current, Token::Ident("x".to_string()));
        tokens.next();
        assert_eq!(tokens.current, Token::Operator(Operator::GE));
        tokens.next();
        assert_eq!(tokens.current, Token::Value(Value::Int(5i64)));
        tokens.next();
        assert_eq!(tokens.current, Token::Seperator(Seperator::RParen));
        tokens.next();
        assert_eq!(tokens.current, Token::EOF);

        Ok(())
    }
}
