use std::{iter::Peekable, str::Chars};

use tracing::debug;

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
            current: Token::Eof,
            next: Token::Eof,
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
            Token::Eof => None,
            token => Some(token),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn peek(&self) -> Option<&Token> {
        match &self.next {
            Token::Eof => None,
            token => Some(token),
        }
    }

    pub fn current(&self) -> Option<&Token> {
        match &self.current {
            Token::Eof => None,
            token => Some(token),
        }
    }

    fn next_token(&mut self) -> Token {
        if self.empty {
            // debug!("returning EOF Token");
            return Token::Eof;
        }

        let iter = &mut self.input;
        let mut substring: String = String::new();
        eat_whitespace(iter);

        // check for single character token
        if let Some(t) = lex_char(iter) {
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
        return lex_keyword(&substring).unwrap_or_else(|| lex_value(&substring));
        // debug!(?t, "returning token");
        // debug!(char = ?iter.peek(), "next char");
    }
}

// nom nom nom
fn eat_whitespace(iter: &mut Peekable<Chars<'_>>) {
    // debug!("skipping whitespace");
    while let Some(c) = iter.peek() {
        if c.is_whitespace() {
            iter.next();
        } else {
            return;
        }
    }
}

fn lex_char(iter: &mut Peekable<Chars<'_>>) -> Option<Token> {
    // debug!("parsing char");
    match iter.peek() {
        Some(c) => match *c {
            LPAREN => Some(Token::Seperator(Seperator::LParen)),
            RPAREN => Some(Token::Seperator(Seperator::RParen)),
            COMMA => Some(Token::Seperator(Seperator::Comma)),
            SEMICOLON => Some(Token::Seperator(Seperator::Semicolon)),

            ASSIGN => {
                iter.next();
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::Equal))
                } else {
                    Some(Token::Operator(Operator::Assign))
                }
            }
            PLUS => Some(Token::Operator(Operator::Plus)),
            MINUS => Some(Token::Operator(Operator::Minus)),
            MULTI => Some(Token::Operator(Operator::Multi)),
            DIVIDE => Some(Token::Operator(Operator::Divide)),
            MODULO => Some(Token::Operator(Operator::Modulo)),
            LT => {
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::Le))
                } else {
                    Some(Token::Operator(Operator::Lt))
                }
            }
            GT => {
                iter.next();
                if let Some(c) = iter.peek()
                    && let ASSIGN = *c
                {
                    iter.next();
                    Some(Token::Operator(Operator::Ge))
                } else {
                    Some(Token::Operator(Operator::Gt))
                }
            }

            _ => None,
        },
        None => Some(Token::Eof),
    }
}

// is the next char an operator?
fn is_operator(ch: char) -> bool {
    // debug!(%ch,"peeking char");
    match ch {
        LPAREN => true,
        RPAREN => true,
        COMMA => true,
        SEMICOLON => true,

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

fn lex_keyword(string: &str) -> Option<Token> {
    KEYWORDS.with(|e| {
        if let Some(k) = e.get(string.to_ascii_lowercase().as_str()) {
            // debug!("Keyword found");
            Some(Token::Keyword(*k))
        } else {
            None
        }
    })
}

fn lex_value(string: &str) -> Token {
    if string.starts_with('"') && string.ends_with('"') {
        return Token::Value(Value::Str(string.trim_matches('"').to_string()));
    }
    if let Ok(int) = string.parse::<i64>() {
        Token::Value(Value::Int(string.to_owned()))
    } else {
        Token::Ident(string.to_string())
    }
}

#[cfg(test)]
mod lexer_test {
    use super::*;
    use test_log::test;

    #[test]
    fn token_test() {
        let input = "SELECT * FROM my_table";
        let mut lexer = Lexer::new(input);

        let t1 = lexer.next_token();
        let t2 = lexer.next_token();
        let t3 = lexer.next_token();
        let t4 = lexer.next_token();
        let t5 = lexer.next_token();

        assert_eq!(t1, Token::Keyword(Keyword::Select));
        assert_eq!(t2, Token::Operator(Operator::Multi));
        assert_eq!(t3, Token::Keyword(Keyword::From));
        assert_eq!(t4, Token::Ident("my_table".to_string()));
        assert_eq!(t5, Token::Eof);
    }

    #[test]
    fn token_test2() {
        let input = "SELECT name FROM my_table WHERE (x >= 5);";
        let mut tokens = Lexer::new(input);

        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::Select));
        assert_eq!(*tokens.next().unwrap(), Token::Ident("name".to_string()));
        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::From));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Ident("my_table".to_string())
        );
        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::Where));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::LParen));
        assert_eq!(*tokens.next().unwrap(), Token::Ident("x".to_string()));
        assert_eq!(*tokens.next().unwrap(), Token::Operator(Operator::Ge));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Value(Value::Int(5i64.to_string()))
        );
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::RParen));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Seperator(Seperator::Semicolon)
        );

        assert!(tokens.next().is_none());
        assert_eq!(tokens.current, Token::Eof);
    }

    #[test]
    fn token_test_insert1() {
        let input = "INSERT INTO table (col1, col2) VALUES (2*2), \"Hello\";";
        let mut tokens = Lexer::new(input);

        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::Insert));
        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::Into));
        assert_eq!(*tokens.next().unwrap(), Token::Ident("table".to_string()));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::LParen));
        assert_eq!(*tokens.next().unwrap(), Token::Ident("col1".to_string()));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::Comma));
        assert_eq!(*tokens.next().unwrap(), Token::Ident("col2".to_string()));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::RParen));
        assert_eq!(*tokens.next().unwrap(), Token::Keyword(Keyword::Values));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::LParen));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Value(Value::Int(2.to_string()))
        );
        assert_eq!(*tokens.next().unwrap(), Token::Operator(Operator::Multi));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Value(Value::Int(2.to_string()))
        );
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::RParen));
        assert_eq!(*tokens.next().unwrap(), Token::Seperator(Seperator::Comma));
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Value(Value::Str("Hello".to_string()))
        );
        assert_eq!(
            *tokens.next().unwrap(),
            Token::Seperator(Seperator::Semicolon)
        );

        assert!(tokens.next().is_none());
        assert_eq!(tokens.current, Token::Eof);
    }
}
