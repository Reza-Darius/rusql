use std::fmt::Debug;
use tracing::{debug, info};

use crate::database::errors::*;
use crate::interpreter::parser::eval::*;
use crate::interpreter::parser::statement::*;
use crate::interpreter::parser::types::*;
use crate::interpreter::{lexer::*, tokens::*};

pub struct Parser<'a> {
    pub lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        let parser = Parser {
            lexer: Lexer::new(input),
        };
        parser
    }

    pub fn next(&mut self) -> Option<&Token> {
        self.lexer.next()
    }

    pub fn peek(&self) -> Option<&Token> {
        self.lexer.peek()
    }

    pub fn parse_input(&mut self) -> Result<Vec<Statement>> {
        let mut res = vec![];
        if let Some(t) = self.lexer.next() {
            debug!("parsing {t:?}");
            match t {
                Token::Keyword(Keyword::SELECT) => res.push(parse_select(self)?),
                // Token::Keyword(Keyword::INSERT) => parse_insert(&mut tokens),
                _ => return Err(ParseError::ParseError("invalid input".to_string()).into()),
            }
        } else {
            // error
            return Err(ParseError::ParseError("invalid input".to_string()).into());
        };
        Ok(res)
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
        _ => {
            tracing::error!(?token, "unexpected token for get prefix fn");
            panic!()
        }
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

pub fn parse_columns(parser: &mut Parser) -> Result<Columns> {
    if let Some(t) = parser.peek()
        && let Token::Operator(Operator::MULTI) = t
    {
        return Ok(Columns::Wildcard);
    }

    let mut cols: Vec<String> = vec![];
    while let Some(t) = parser.peek() {
        debug!("parsing {t:?}");
        match t {
            Token::Ident(i) => {
                cols.push(i.clone());
                parser.next();
            }
            Token::Keyword(_) => return Ok(Columns::Cols(cols)),
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
pub fn parse_identifier(parser: &mut Parser) -> Result<String> {
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

pub fn parse_index(parser: &mut Parser) -> Result<Vec<Index>> {
    let mut result = vec![];

    let column = parse_identifier(parser)?;
    let operator = parse_operator(parser)?;
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

pub fn parse_operator(parser: &mut Parser) -> Result<Operator> {
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

pub fn parse_limit(parser: &mut Parser) -> Result<Box<dyn Expression>> {
    info!("parsing LIMIT clause");

    parse_expression_statement(parser)
        .ok_or_else(|| ParseError::ParseError("parsing LIMIT clause failed".to_string()).into())
}

pub fn parse_expression_statement(parser: &mut Parser) -> Option<Box<dyn Expression>> {
    info!(?parser.lexer.current, ?parser.lexer.next, "parsing expression statement");

    parser.next();
    let expr = parser.parse_expression(Precedence::Lowest);
    expr
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

    expr.unwrap()
}

#[cfg(test)]
mod parser_test {
    use super::*;
    use test_log::test;

    #[test]
    fn parser_test() {
        let input = "10 + 10";
        let mut parser = Parser::new(input);
        let expr = parse_expression_statement(&mut parser)
            .unwrap()
            .evaluate()
            .unwrap();

        assert_eq!(expr, ValueObject::Int(20));

        let input = "(-(10 - 5) * 2)";
        let mut parser = Parser::new(input);
        let expr = parse_expression_statement(&mut parser)
            .unwrap()
            .evaluate()
            .unwrap();

        assert_eq!(expr, ValueObject::Int(-10));
    }
}
