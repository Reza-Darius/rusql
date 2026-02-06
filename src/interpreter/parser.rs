use std::collections::HashMap;
use std::fmt::Debug;
use tracing::{debug, info};

use crate::database::errors::{Error, ParseError, Result};
use crate::interpreter::{lexer::*, tokens::*};

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

type PrefixParseFn = fn(parser: &mut Parser) -> Box<dyn Expression>;
type InfixParseFn = fn(parser: &mut Parser, lhs: Box<dyn Expression>) -> Box<dyn Expression>;

struct Parser<'a> {
    lexer: Lexer<'a>,

    prefix_fns: HashMap<Token, PrefixParseFn>,
    infix_fns: HashMap<Token, InfixParseFn>,
    precedence: HashMap<Token, Precedence>,
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
        parser
            .precedence
            .insert(Token::Operator(Operator::PLUS), Precedence::Sum);
        parser
            .precedence
            .insert(Token::Operator(Operator::MINUS), Precedence::Sum);
        parser
            .precedence
            .insert(Token::Operator(Operator::ASSIGN), Precedence::Equals);
        parser
            .precedence
            .insert(Token::Operator(Operator::EQUAL), Precedence::Equals);

        parser
            .precedence
            .insert(Token::Operator(Operator::DIVIDE), Precedence::Product);
        parser
            .precedence
            .insert(Token::Operator(Operator::MULTI), Precedence::Product);

        parser
            .precedence
            .insert(Token::Operator(Operator::GT), Precedence::LessGreater);
        parser
            .precedence
            .insert(Token::Operator(Operator::GE), Precedence::LessGreater);
        parser
            .precedence
            .insert(Token::Operator(Operator::LT), Precedence::LessGreater);
        parser
            .precedence
            .insert(Token::Operator(Operator::LE), Precedence::LessGreater);

        // populating function calls
        parser
            .prefix_fns
            .insert(Token::Operator(Operator::MINUS), parse_prefix_expression);
        parser.prefix_fns.insert(
            Token::Seperator(Seperator::LParen),
            parse_grouped_expression,
        );

        parser
            .infix_fns
            .insert(Token::Operator(Operator::PLUS), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::DIVIDE), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::MULTI), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::EQUAL), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::GT), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::GE), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::LT), parse_infix_expression);
        parser
            .infix_fns
            .insert(Token::Operator(Operator::LE), parse_infix_expression);
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
        *self
            .precedence
            .get(&self.lexer.current)
            .unwrap_or_else(|| &Precedence::Lowest)
    }

    fn prec_next(&self) -> Precedence {
        *self
            .precedence
            .get(&self.lexer.next)
            .unwrap_or_else(|| &Precedence::Lowest)
    }

    fn parse_expression(&mut self, prec: Precedence) -> Option<Box<dyn Expression>> {
        let mut left_expr: Box<dyn Expression> = match &self.lexer.current {
            Token::EOF => return None,
            t => {
                debug!("parsing {t:?}");
                match t {
                    Token::Value(Value::Int(i)) => Box::new(*i),
                    Token::Value(Value::Str(s)) => Box::new(s.clone()),
                    t => self.prefix_fns[t](self),
                }
            }
        };
        let end_expr = match &self.lexer.next {
            Token::Keyword(_) => true,
            _ => false,
        };
        while !end_expr && prec < self.prec_next() {
            let infix_fn = self.infix_fns.get(&self.lexer.next);
            if infix_fn.is_none() {
                return Some(left_expr);
            }
            self.lexer.next();
            left_expr = infix_fn.unwrap()(self, left_expr)
        }

        Some(left_expr)
    }
}

#[derive(Debug)]
pub struct SelectStatement {
    columns: Vec<String>,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<u64>,
}

// SELECT col1, col2 FROM table WHERE col1 = (10 + (1 * 2)) AND col2 > 5 LIMIT -5 + 7
fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");
    let tokens = &mut parser.lexer;

    let mut statement = SelectStatement {
        columns: vec![],
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(tokens)?;

    // parsing FROM
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

    // parsing table ident
    statement.table = parse_identifier(tokens)?;

    tokens.next();
    if tokens.is_empty() {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match tokens.current {
        Token::Keyword(Keyword::WHERE) => {
            debug!("parsing WHERE");
            statement.index = Some(parse_index(parser)?);
            ()
        }
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT");
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

    while let Some(t) = tokens.peek() {
        debug!("parsing {t:?}");
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
                tokens.next();
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
    let mut result = vec![];

    let column = parse_identifier(&mut parser.lexer)?;
    let operator = parse_operator(&mut parser.lexer)?;
    parser.lexer.next();
    let expr = parse_expression_statement(parser)
        .ok_or_else(|| ParseError::ParseError("couldnt parse expression".to_string()))?;

    let index = Index {
        column,
        operator,
        expr,
    };
    result.push(index);
    Ok(result)
}

fn parse_operator(tokens: &mut Lexer) -> Result<Operator> {
    debug!("parsing operator");
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

fn parse_expression_statement(parser: &mut Parser) -> Option<Box<dyn Expression>> {
    info!(?parser.lexer.current, ?parser.lexer.next, "parsing expression statement");
    let expr = parser.parse_expression(Precedence::Lowest);
    expr
}

#[derive(Debug)]
struct PrefixExpression {
    operator: Operator,
    rhs: Option<Box<dyn Expression>>,
}

impl Expression for PrefixExpression {}

fn parse_prefix_expression(parser: &mut Parser) -> Box<dyn Expression> {
    let mut expr = match &parser.lexer.current {
        Token::Operator(op) => Box::new(PrefixExpression {
            operator: *op,
            rhs: None,
        }),
        _ => panic!("unexpected token"),
    };
    parser.lexer.next();
    expr.rhs = parser.parse_expression(Precedence::Prefix);
    expr
}

#[derive(Debug)]
struct InfixExpression {
    lhs: Option<Box<dyn Expression>>,
    operator: Operator,
    rhs: Option<Box<dyn Expression>>,
}

impl Expression for InfixExpression {}

fn parse_infix_expression(parser: &mut Parser, lhs: Box<dyn Expression>) -> Box<dyn Expression> {
    info!("parsing infix expression");
    let mut expr = match &parser.lexer.current {
        Token::Operator(op) => Box::new(InfixExpression {
            lhs: Some(lhs),
            operator: *op,
            rhs: None,
        }),
        _ => panic!("unexpected token"),
    };
    let prec = parser.prec_current();
    parser.lexer.next();
    expr.rhs = parser.parse_expression(prec);
    expr
}

fn parse_grouped_expression(parser: &mut Parser) -> Box<dyn Expression> {
    parser.lexer.next();
    let expr = parser.parse_expression(Precedence::Lowest);
    if parser.lexer.next != Token::Seperator(Seperator::RParen) {
        panic!("expected closing parantheses")
    }
    expr.unwrap()
}

#[derive(Debug)]
pub struct InsertStatement;
#[derive(Debug)]
pub struct UpdateStatement;
#[derive(Debug)]
pub struct DeleteStatement;
#[derive(Debug)]
pub struct CreateStatement;
#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
enum Precedence {
    Lowest,
    Equals,
    LessGreater,
    Sum,
    Product,
    Prefix,
}

#[cfg(test)]
mod parser_test {
    use super::*;
    use test_log::test;

    #[test]
    fn parser_test1() {
        let input =
            "SELECT col1, col2 FROM table WHERE col1 = (10 + (1 * 2)) AND col2 > 5 LIMIT -5 + 7";
        let mut parser = Parser::new(input);
        let res = parser.parse_input().unwrap();
        println!("{:?}", res);
    }
}
