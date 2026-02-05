use std::collections::HashMap;
use std::fmt::Debug;
use tracing::debug;

use crate::database::errors::{Error, ParseError, Result};
use crate::interpreter::{lexer::*, tokens::*};

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

type PrefixParseFn = fn(parser: &mut Parser) -> Box<dyn Expression>;
type InfixParseFn = fn(parser: &mut Parser, lhs: &dyn Expression) -> Box<dyn Expression>;

struct Parser<'a> {
    lexer: Lexer<'a>,

    prefix_fns: HashMap<Token, PrefixParseFn>,
    infix_fns: HashMap<Token, InfixParseFn>,
    precedence: HashMap<Operator, Precedence>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        let mut parser = Parser {
            lexer: Lexer::new(input),
            prefix_fns: HashMap::new(),
            infix_fns: HashMap::new(),
            precedence: HashMap::new(),
        };

        // populating precedence map
        parser.precedence.insert(Operator::PLUS, Precedence::Sum);
        parser.precedence.insert(Operator::MINUS, Precedence::Sum);

        parser
            .precedence
            .insert(Operator::ASSIGN, Precedence::Equals);
        parser
            .precedence
            .insert(Operator::EQUAL, Precedence::Equals);

        parser
            .precedence
            .insert(Operator::DIVIDE, Precedence::Product);
        parser
            .precedence
            .insert(Operator::MULTI, Precedence::Product);

        parser
            .precedence
            .insert(Operator::GT, Precedence::LessGreater);
        parser
            .precedence
            .insert(Operator::GE, Precedence::LessGreater);
        parser
            .precedence
            .insert(Operator::LT, Precedence::LessGreater);
        parser
            .precedence
            .insert(Operator::LE, Precedence::LessGreater);

        // populating function calls
        parser
            .prefix_fns
            .insert(Token::Operator(Operator::MINUS), parse_prefix_expression);

        parser
    }

    pub fn parse_input(&mut self) -> Result<Statement> {
        if let Some(t) = self.lexer.next() {
            debug!("parsing {t:?}");
            match t {
                Token::Keyword(Keyword::SELECT) => parse_select(self),
                // Token::Keyword(Keyword::INSERT) => parse_insert(&mut tokens),
                _ => Err(ParseError::ParseError("invalid input".to_string()).into()),
            }
        } else {
            // error
            Err(ParseError::ParseError("invalid input".to_string()).into())
        }
    }

    fn prec_current(&self) -> Precedence {
        let cur = match &self.lexer.current {
            Token::Operator(op) => op,
            _ => panic!("unexpected token, expected operator"),
        };
        *self.precedence.get(cur).expect("we know it exists")
    }

    fn prec_next(&self) -> Precedence {
        let cur = match &self.lexer.next {
            Token::Operator(op) => op,
            _ => panic!("unexpected token, expected operator"),
        };
        *self.precedence.get(cur).expect("we know it exists")
    }

    fn parse_expression(&mut self, prec: Precedence) -> Option<Box<dyn Expression>> {
        let expr_fn = match self.lexer.next() {
            Some(t) => {
                debug!("parsing {t:?}");
                match t {
                    Token::Value(Value::Int(i)) => return Some(Box::new(*i)),
                    Token::Value(Value::Str(s)) => return Some(Box::new(s.clone())),
                    t => self.prefix_fns[t],
                }
            }
            None => return None,
        };

        Some(expr_fn(self))
    }
}

pub struct SelectStatement {
    columns: Vec<String>,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<u64>,
}

// SELECT col FROM table WHERE col1 = (10 + (1 * 2)) AND col2 > 5 LIMIT -5 + 7
fn parse_select(parser: &mut Parser) -> Result<Statement> {
    let tokens = &mut parser.lexer;

    let mut statement = SelectStatement {
        columns: vec![],
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(tokens)?;
    statement.table = parse_identifier(tokens)?;

    tokens.next();
    if tokens.is_empty() {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match tokens.current {
        Token::Keyword(Keyword::WHERE) => {
            statement.index = Some(parse_index(parser)?);
            ()
        }
        Token::Keyword(Keyword::LIMIT) => {
            statement.limit = Some(parse_limit(parser)?);
            return Ok(Statement::Select(statement));
        }
        ref t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE or LIMIT clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    todo!()
}

fn parse_columns(tokens: &mut Lexer) -> Result<Vec<String>> {
    let mut cols: Vec<String> = vec![];

    while let Some(t) = tokens.next() {
        debug!("parsing {t:?}");
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
            }
            Token::Keyword(_) => return Ok(cols),
            Token::Seperator(Seperator::Comma) => continue,
            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected comma or closing parantheses".to_string(),
                    got: t.to_string(),
                }
                .into());
            }
        }
    }

    Err(ParseError::ParseError("missing token".to_string()).into())
}

// columns and table names
fn parse_identifier(tokens: &mut Lexer) -> Result<String> {
    if let Some(t) = tokens.next() {
        debug!("parsing {t:?}");
        if *t != Token::Keyword(Keyword::FROM) {
            return Err(ParseError::InvalidToken {
                expected: "expected FROM keyword".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    } else {
        return Err(ParseError::ParseError("invalid token".to_string()).into());
    }

    if let Some(t) = tokens.next() {
        debug!("parsing {t:?}");
        match t {
            Token::Ident(i) => Ok(i.clone()),
            t => Err(ParseError::InvalidToken {
                expected: "expected table identifier".to_string(),
                got: t.to_string(),
            }
            .into()),
        }
    } else {
        Err(ParseError::ParseError("missing token".to_string()).into())
    }
}

fn parse_index(parser: &mut Parser) -> Result<Vec<Index>> {
    // parse identifier
    let mut index = Index {
        column: parse_identifier(&mut parser.lexer)?,
        operator: parse_operator(&mut parser.lexer)?,
        expr: todo!(),
    };
    // parse equal operator
    // parse expression
    let expr = parser.parse_expression(Precedence::Lowest);
    todo!()
}

fn parse_operator(tokens: &mut Lexer) -> Result<Operator> {
    if let Some(t) = tokens.next() {
        match t {
            Token::Operator(Operator::ASSIGN) => Ok(Operator::EQUAL),
            Token::Operator(Operator::EQUAL) => Ok(Operator::EQUAL),
            Token::Operator(Operator::GE) => Ok(Operator::GE),
            Token::Operator(Operator::GT) => Ok(Operator::GT),
            Token::Operator(Operator::LE) => Ok(Operator::LE),
            Token::Operator(Operator::LT) => Ok(Operator::LT),

            t => Err(ParseError::InvalidToken {
                expected: "comparison operator".to_string(),
                got: t.to_string(),
            }
            .into()),
        }
    } else {
        Err(ParseError::ParseError("missing token".to_string()).into())
    }
}

fn parse_limit(parser: &mut Parser) -> Result<u64> {
    // parse expression
    todo!()
}

#[derive(Debug)]
struct PrefixExpression {
    operator: Operator,
    rhs: Box<dyn Expression>,
}

impl Expression for PrefixExpression {}

fn parse_prefix_expression(parser: &mut Parser) -> Box<dyn Expression> {
    match &parser.lexer.current {
        Token::Operator(op) => Box::new(PrefixExpression {
            operator: *op,
            rhs: parser.parse_expression(Precedence::Prefix).unwrap(),
        }),
        _ => panic!("unexpected token"),
    }
}

#[derive(Debug)]
struct InfixExpression {
    lhs: Box<dyn Expression>,
    operator: Token,
    rhs: Box<dyn Expression>,
}

impl Expression for InfixExpression {}

fn parse_infix_expression(parser: &mut Parser) -> Box<dyn Expression> {
    todo!()
}

pub struct InsertStatement;
pub struct UpdateStatement;
pub struct DeleteStatement;
pub struct CreateStatement;

struct Index {
    column: String,
    operator: Operator,
    expr: Box<dyn Expression>,
}

trait Expression: Debug {}

impl Expression for Value {}
impl Expression for &str {}
impl Expression for String {}
impl Expression for i64 {}

struct IntLiteral(i64);
struct StrLiteral(String);

#[derive(Debug, Clone, Copy)]
enum Precedence {
    Lowest,
    Equals,
    LessGreater,
    Sum,
    Product,
    Prefix,
}
