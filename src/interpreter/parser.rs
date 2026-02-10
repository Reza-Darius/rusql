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

// type PrefixParseFn = fn(parser: &mut Parser) -> Box<dyn Expression>;
// type InfixParseFn = fn(parser: &mut Parser, lhs: Box<dyn Expression>) -> Box<dyn Expression>;

struct Parser<'a> {
    lexer: Lexer<'a>,
    // prefix_fns: HashMap<Token, PrefixParseFn>,
    // infix_fns: HashMap<Token, InfixParseFn>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        let parser = Parser {
            lexer: Lexer::new(input),
            // prefix_fns: HashMap::new(),
            // infix_fns: HashMap::new(),
        };

        // // populating function calls
        // // prefix
        // parser
        //     .prefix_fns
        //     .insert(Token::Operator(Operator::MINUS), parse_prefix_expression);
        // parser.prefix_fns.insert(
        //     Token::Seperator(Seperator::LParen),
        //     parse_grouped_expression,
        // );
        // // infix
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::PLUS), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::DIVIDE), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::MULTI), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::EQUAL), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::GT), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::GE), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::LT), parse_infix_expression);
        // parser
        //     .infix_fns
        //     .insert(Token::Operator(Operator::LE), parse_infix_expression);

        parser
    }

    fn next(&mut self) -> Option<&Token> {
        self.lexer.next()
    }

    fn peek(&mut self) -> Option<&Token> {
        self.lexer.peek()
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
        check_prec(&self.lexer.current)
        // *self
        //     .precedence
        //     .get(&self.lexer.current)
        //     .unwrap_or_else(|| &Precedence::Lowest)
    }

    fn prec_next(&self) -> Precedence {
        check_prec(&self.lexer.next)
        // *self
        //     .precedence
        //     .get(&self.lexer.next)
        //     .unwrap_or_else(|| &Precedence::Lowest)
    }

    fn parse_expression(&mut self, prec: Precedence) -> Option<Box<dyn Expression>> {
        debug!(?prec, "parse expression with prec:");
        let mut left_expr: Box<dyn Expression> = match &self.lexer.current {
            Token::EOF => return None,
            t => {
                debug!("parsing {t:?}");
                match t {
                    Token::Value(Value::Int(i)) => Box::new(IntLiteral(i.clone())),
                    Token::Value(Value::Str(s)) => Box::new(StrLiteral(s.clone())),
                    t => get_prefix_fn(t)(self),
                }
            }
        };

        // are we at the end of an expression?
        let end_expr = match &self.lexer.next {
            Token::Keyword(_) => true,
            Token::Seperator(Seperator::Semicolon) => true,
            _ => false,
        };

        debug!(
            "comparing prec {:?} with prec_next {:?} of token {:?}",
            prec,
            self.prec_next(),
            self.peek()
        );

        while !end_expr && prec < self.prec_next() {
            if let Some(infix_fn) = get_infix_fn(&self.lexer.next) {
                self.lexer.next();
                left_expr = infix_fn(self, left_expr)
            } else {
                debug!(
                    ?left_expr,
                    "no infix function found, returning left expression"
                );
                return Some(left_expr);
            }
        }

        debug!(?left_expr, "returning left expression");
        Some(left_expr)
    }
}

fn check_prec(token: &Token) -> Precedence {
    match *token {
        Token::Operator(Operator::PLUS) => Precedence::Sum,
        Token::Operator(Operator::MINUS) => Precedence::Sum,
        Token::Operator(Operator::MULTI) => Precedence::Product,
        Token::Operator(Operator::DIVIDE) => Precedence::Product,

        Token::Operator(Operator::EQUAL) => Precedence::Equals,

        Token::Operator(Operator::GT) => Precedence::LessGreater,
        Token::Operator(Operator::GE) => Precedence::LessGreater,
        Token::Operator(Operator::LT) => Precedence::LessGreater,
        Token::Operator(Operator::LE) => Precedence::LessGreater,
        _ => Precedence::Lowest,
    }
}

fn get_prefix_fn(token: &Token) -> fn(parser: &mut Parser) -> Box<dyn Expression> {
    match token {
        Token::Operator(Operator::MINUS) => parse_prefix_expression,
        Token::Seperator(Seperator::LParen) => parse_grouped_expression,
        _ => panic!("unexpected token for get prefix fn"),
    }
}

fn get_infix_fn(
    token: &Token,
) -> Option<fn(parser: &mut Parser, lhs: Box<dyn Expression>) -> Box<dyn Expression>> {
    match token {
        Token::Operator(Operator::PLUS) => Some(parse_infix_expression),
        Token::Operator(Operator::MINUS) => Some(parse_infix_expression),
        Token::Operator(Operator::MULTI) => Some(parse_infix_expression),
        Token::Operator(Operator::EQUAL) => Some(parse_infix_expression),
        Token::Operator(Operator::MODULO) => Some(parse_infix_expression),

        Token::Operator(Operator::GT) => Some(parse_infix_expression),
        Token::Operator(Operator::GE) => Some(parse_infix_expression),
        Token::Operator(Operator::LT) => Some(parse_infix_expression),
        Token::Operator(Operator::LE) => Some(parse_infix_expression),
        _ => None,
    }
}

#[derive(Debug)]
pub struct SelectStatement {
    columns: Vec<String>,
    table: String,
    index: Option<Vec<Index>>,
    limit: Option<Box<dyn Expression>>,
}

// SELECT col1, col2 FROM table WHERE col1 = (10 + (1 * 2)) AND col2 > 5 LIMIT -5 + 7;
fn parse_select(parser: &mut Parser) -> Result<Statement> {
    info!("parsing SELECT statement!");

    let mut statement = SelectStatement {
        columns: vec![],
        table: String::new(),
        index: None,
        limit: None,
    };

    statement.columns = parse_columns(parser)?;

    // parsing FROM
    if let Some(t) = parser.next() {
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
    statement.table = parse_identifier(parser)?;

    parser.next();
    if parser.lexer.current == Token::Seperator(Seperator::Semicolon) {
        return Ok(Statement::Select(statement));
    }

    // optional index or limit clause
    match parser.lexer.current {
        Token::Keyword(Keyword::WHERE) => {
            debug!("parsing WHERE clause");
            statement.index = Some(parse_index(parser)?);
            ()
        }
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT clause");
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

    match parser.lexer.current {
        Token::Keyword(Keyword::LIMIT) => {
            debug!("parsing LIMIT clause");
            statement.limit = Some(parse_limit(parser)?);
        }
        ref t => {
            return Err(ParseError::InvalidToken {
                expected: "expected WHERE clause".to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    Ok(Statement::Select(statement))
}

fn parse_keyword(parser: &mut Parser, expected: Token) -> Result<()> {
    if let Some(t) = parser.next() {
        debug!("parsing {t:?}");
        if *t == expected {
            return Ok(());
        } else {
            return Err(ParseError::InvalidToken {
                expected: expected.to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };
    return Err(ParseError::ParseError("expected token".to_string()).into());
}

fn parse_columns(parser: &mut Parser) -> Result<Vec<String>> {
    let mut cols: Vec<String> = vec![];

    while let Some(t) = parser.peek() {
        debug!("parsing {t:?}");
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
                parser.next();
            }
            Token::Keyword(_) => return Ok(cols),
            Token::Seperator(Seperator::Comma) => {
                parser.next();
                continue;
            }
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
fn parse_identifier(parser: &mut Parser) -> Result<String> {
    if let Some(t) = parser.next() {
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

    let column = parse_identifier(parser)?;
    let operator = parse_operator(parser)?;
    parser.lexer.next();
    let expr = parse_expression_statement(parser)
        .ok_or_else(|| ParseError::ParseError("couldnt parse expression".to_string()))?;

    let index = Index {
        column,
        operator,
        expr,
    };
    result.push(index);
    parser.next();
    Ok(result)
}

fn parse_operator(parser: &mut Parser) -> Result<Operator> {
    debug!("parsing operator");
    if let Some(t) = parser.next() {
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

fn parse_limit(parser: &mut Parser) -> Result<Box<dyn Expression>> {
    info!("parsing LIMIT clause");
    parser.next();
    parse_expression_statement(parser)
        .ok_or_else(|| ParseError::ParseError("parsing LIMIT clause failed".to_string()).into())
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

impl Expression for PrefixExpression {
    fn into_type(self) -> ExprType {
        ExprType::Prefix(self)
    }
}

fn parse_prefix_expression(parser: &mut Parser) -> Box<dyn Expression> {
    let mut expr = match &parser.lexer.current {
        Token::Operator(op) => Box::new(PrefixExpression {
            operator: *op,
            rhs: None,
        }),
        _ => panic!("unexpected prefix token"),
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

impl Expression for InfixExpression {
    fn into_type(self) -> ExprType {
        ExprType::Infix(self)
    }
}

fn parse_infix_expression(parser: &mut Parser, lhs: Box<dyn Expression>) -> Box<dyn Expression> {
    info!("parsing infix expression");
    let mut expr = match &parser.lexer.current {
        Token::Operator(op) => Box::new(InfixExpression {
            lhs: Some(lhs),
            operator: *op,
            rhs: None,
        }),
        _ => panic!("unexpected infix token"),
    };
    let prec = parser.prec_current();
    parser.lexer.next();
    expr.rhs = parser.parse_expression(prec);
    expr
}

fn parse_grouped_expression(parser: &mut Parser) -> Box<dyn Expression> {
    debug!("parsing grouped expression");
    parser.next();
    let expr = parser.parse_expression(Precedence::Lowest);
    if parser.lexer.next != Token::Seperator(Seperator::RParen) {
        panic!("expected closing parantheses")
    }
    parser.next();
    debug!("returning grouped expression");
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

enum ExprType {
    Infix(InfixExpression),
    Prefix(PrefixExpression),
    IntLiteral(String),
    StrLiteral(String),
}

trait Expression: Debug {
    fn into_type(self) -> ExprType;
}

#[derive(Debug)]
struct IntLiteral(String);

impl Expression for IntLiteral {
    fn into_type(self) -> ExprType {
        ExprType::IntLiteral(self.0)
    }
}

#[derive(Debug)]
struct StrLiteral(String);

impl Expression for StrLiteral {
    fn into_type(self) -> ExprType {
        ExprType::StrLiteral(self.0)
    }
}

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
        let input = "SELECT col1, col2 FROM table WHERE col1 = ((2 * (10 + 1)) * 2) LIMIT -5 + 7";
        let mut parser = Parser::new(input);
        let res = parser.parse_input().unwrap();
        println!("{:?}", res);
    }
}
