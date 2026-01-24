use std::{collections::HashMap, iter::Peekable, str::Chars};

use tracing::debug;

use crate::database::errors::*;

use crate::interpreter::tokens::*;

thread_local! {
    static KEYWORDS: HashMap<&'static str, Keyword> =  {
        let mut map = HashMap::new();
        map.insert(SELECT, Keyword::SELECT);
        map.insert(INSERT, Keyword::INSERT);
        map.insert(UPDATE, Keyword::UPDATE);
        map.insert(DELETE, Keyword::DELETE);

        map.insert(FROM, Keyword::FROM);
        map.insert(INTO, Keyword::INTO);
        map.insert(WHERE, Keyword::WHERE);
        map.insert(LIMIT, Keyword::LIMIT);
        map
    }
}

struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
}

// impl<'a> Iterator for Lexer<'a> {
//     type Item = Token;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Lexer {
            input: input.chars().peekable(),
        }
    }

    fn next_token(&mut self) -> Token {
        let mut substring: String = String::new();
        skip_whitespace(&mut self.input);

        // check for single character token
        match self.input.next() {
            Some(c) => match parse_char(&c) {
                Some(t) => {
                    debug!(?t, "returning token");
                    return t;
                }
                None => substring.push(c),
            },
            None => {
                debug!("returning EOF token");
                return Token::EOF;
            }
        }

        // create substring
        while let Some(c) = self.input.next() {
            if !c.is_whitespace() {
                substring.push(c);
            } else {
                break;
            }
        }
        parse_keyword(&substring).unwrap_or_else(|| Token::Value(substring))
    }
}

fn skip_whitespace(iter: &mut Peekable<Chars<'_>>) {
    while let Some(c) = iter.peek() {
        if c.is_whitespace() {
            iter.next();
        }
    }
}

fn parse_char(char: &char) -> Option<Token> {
    match *char {
        LPAREN => Some(Token::Seperator(Seperator::LParen)),
        RPAREN => Some(Token::Seperator(Seperator::RParen)),
        COMMA => Some(Token::Seperator(Seperator::Comma)),

        ASSIGN => Some(Token::Operand(Operator::ASSIGN)),
        PLUS => Some(Token::Operand(Operator::PLUS)),
        MINUS => Some(Token::Operand(Operator::MINUS)),
        MULTI => Some(Token::Operand(Operator::MULTI)),
        DIVIDE => Some(Token::Operand(Operator::DIVIDE)),
        MODULO => Some(Token::Operand(Operator::MODULO)),

        _ => None,
    }
}

fn parse_keyword(string: &str) -> Option<Token> {
    KEYWORDS.with(|e| {
        if let Some(k) = e.get(string) {
            debug!(?k, "Keyword found");
            Some(Token::Keyword(*k))
        } else {
            None
        }
    })
}

#[cfg(test)]
mod lexer_test {
    use super::*;
    use test_log::test;

    #[test]
    fn token_test() -> Result<()> {
        let input = "SELECT * FROM my_table";
        let mut lexer = Lexer::new(input);

        let t1 = lexer.next_token();
        let t2 = lexer.next_token();
        let t3 = lexer.next_token();
        let t4 = lexer.next_token();
        let t5 = lexer.next_token();

        assert_eq!(t1, Token::Keyword(Keyword::SELECT));
        assert_eq!(t2, Token::Value("*".to_string()));
        assert_eq!(t3, Token::Keyword(Keyword::FROM));
        assert_eq!(t4, Token::Value("my_table".to_string()));
        assert_eq!(t5, Token::EOF);
        todo!()
    }
}
