use tracing::error;
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
    pub(super) fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input),
        }
    }

    pub(super) fn next(&mut self) -> Option<&Token> {
        self.lexer.next()
    }

    pub(super) fn peek(&self) -> Option<&Token> {
        self.lexer.peek()
    }

    pub(super) fn current(&self) -> Option<&Token> {
        self.lexer.current()
    }

    pub fn parse(input: &'a str) -> Result<Vec<Statement>> {
        let mut parser = Parser::new(input);
        let mut statements = vec![];

        while let Some(t) = parser.next() {
            debug!("parsing {t:?}");
            match t {
                Token::Eof => break,
                Token::Keyword(Keyword::Select) => statements.push(parse_select(&mut parser)?),
                Token::Keyword(Keyword::Insert) => statements.push(parse_insert(&mut parser)?),
                Token::Keyword(Keyword::Update) => statements.push(parse_update(&mut parser)?),
                Token::Keyword(Keyword::Delete) => statements.push(parse_delete(&mut parser)?),
                Token::Keyword(Keyword::Create) => statements.push(parse_create(&mut parser)?),
                Token::Keyword(Keyword::Drop) => statements.push(parse_drop(&mut parser)?),
                _ => {
                    return Err(ParseError::InvalidToken {
                        expected: "statement keyword".to_string(),
                        got: t.to_string(),
                    }
                    .into());
                }
            }
        }
        Ok(statements)
    }

    fn prec_current(&self) -> Precedence {
        check_prec(&self.lexer.current)
    }

    fn prec_next(&self) -> Precedence {
        check_prec(&self.lexer.next)
    }

    fn parse_expression(&mut self, prec: Precedence) -> Option<Box<dyn Expression>> {
        debug!(?prec, "parse expression with prec:");

        let mut left_expr: Box<dyn Expression> = match self.current()? {
            Token::Eof => return None,
            t => {
                debug!("parsing {t:?} for expression");
                match t {
                    Token::Value(Value::Int(i)) => Box::new(IntLiteral(i.clone())),
                    Token::Value(Value::Str(s)) => Box::new(StrLiteral(s.clone())),
                    t => get_prefix_fn(t)(self),
                }
            }
        };

        debug!(
            "comparing prec {:?} with prec_next {:?} of token {:?}",
            prec,
            self.prec_next(),
            self.peek()
        );

        while prec < self.prec_next() {
            if let Some(infix_fn) = get_infix_fn(self.peek()?) {
                self.next();
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
        Token::Operator(Operator::Plus) => Precedence::Sum,
        Token::Operator(Operator::Minus) => Precedence::Sum,
        Token::Operator(Operator::Multi) => Precedence::Product,
        Token::Operator(Operator::Divide) => Precedence::Product,

        Token::Operator(Operator::Equal) => Precedence::Equals,

        Token::Operator(Operator::Gt) => Precedence::LessGreater,
        Token::Operator(Operator::Ge) => Precedence::LessGreater,
        Token::Operator(Operator::Lt) => Precedence::LessGreater,
        Token::Operator(Operator::Le) => Precedence::LessGreater,
        _ => Precedence::Lowest,
    }
}

fn get_prefix_fn(token: &Token) -> fn(parser: &mut Parser) -> Box<dyn Expression> {
    match token {
        Token::Operator(Operator::Minus) => parse_prefix_expression,
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
        Token::Operator(Operator::Plus) => Some(parse_infix_expression),
        Token::Operator(Operator::Minus) => Some(parse_infix_expression),
        Token::Operator(Operator::Multi) => Some(parse_infix_expression),
        Token::Operator(Operator::Equal) => Some(parse_infix_expression),
        Token::Operator(Operator::Modulo) => Some(parse_infix_expression),

        Token::Operator(Operator::Gt) => Some(parse_infix_expression),
        Token::Operator(Operator::Ge) => Some(parse_infix_expression),
        Token::Operator(Operator::Lt) => Some(parse_infix_expression),
        Token::Operator(Operator::Le) => Some(parse_infix_expression),
        _ => None,
    }
}

pub fn parse_token(parser: &mut Parser, expected: Token) -> Result<()> {
    if let Some(t) = parser.current() {
        debug!("parsing keyword {t:?}");
        if *t == expected {
            parser.next();
            return Ok(());
        } else {
            return Err(ParseError::InvalidToken {
                expected: expected.to_string(),
                got: t.to_string(),
            }
            .into());
        }
    };

    Err(ParseError::ParseError("expected token").into())
}

pub fn parse_columns(parser: &mut Parser) -> Result<StatementColumns> {
    if *parser
        .current()
        .ok_or_else(|| ParseError::ParseError("expected column token"))?
        == Token::Operator(Operator::Multi)
    {
        parser.next();
        return Ok(StatementColumns::Wildcard);
    };

    let mut columns: Vec<String> = vec![];

    while let Some(t) = parser.current() {
        debug!("parsing columns {t:?}");

        match t {
            Token::Ident(i) => {
                columns.push(i.clone());
                parser.next();
            }
            Token::Seperator(Seperator::RParen) => {
                if columns.is_empty() {
                    return Err(ParseError::ParseError("no columns provided!").into());
                } else {
                    parser.next();
                    return Ok(StatementColumns::Cols(columns));
                };
            }
            Token::Keyword(_) => {
                if columns.is_empty() {
                    return Err(ParseError::ParseError("no columns provided!").into());
                } else {
                    return Ok(StatementColumns::Cols(columns));
                };
            }
            Token::Seperator(Seperator::Comma) | Token::Seperator(Seperator::LParen) => {
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

    Err(ParseError::ParseError("missing token").into())
}

// columns and table names
pub fn parse_identifier(parser: &mut Parser) -> Result<String> {
    if let Some(t) = parser.current() {
        debug!("parsing identifier {t:?}");
        match t {
            Token::Ident(i) => {
                let ident = i.clone();
                parser.next();
                Ok(ident)
            }
            t => Err(ParseError::InvalidToken {
                expected: "expected table identifier".to_string(),
                got: t.to_string(),
            }
            .into()),
        }
    } else {
        Err(ParseError::ParseError("missing token").into())
    }
}

pub fn parse_create_column(parser: &mut Parser) -> Result<CreateColumn> {
    let col_name;

    if let Some(t) = parser.current() {
        debug!("parsing creating column for token {t:?}");
        match t {
            Token::Ident(i) => {
                col_name = i.clone();
            }
            t => {
                let err = ParseError::InvalidToken {
                    expected: "expected column identifier".to_string(),
                    got: t.to_string(),
                };
                error!("{err}");
                return Err(err.into());
            }
        }
    } else {
        return Err(ParseError::ParseError("missing token").into());
    }

    parser.next();
    parse_token(parser, Token::Operator(Operator::Assign))?;
    let data_type;

    if let Some(t) = parser.current() {
        debug!("parsing data type for token {t:?}");
        match t {
            Token::Keyword(Keyword::Int) => {
                data_type = DataType::Int;
            }

            Token::Keyword(Keyword::Str) => {
                data_type = DataType::Str;
            }
            t => {
                let err = ParseError::InvalidToken {
                    expected: "expected data type".to_string(),
                    got: t.to_string(),
                };
                error!("{err}");
                return Err(err.into());
            }
        }
    } else {
        return Err(ParseError::ParseError("missing token").into());
    }

    parser.next();

    Ok(CreateColumn {
        col_name,
        data_type,
    })
}
pub fn parse_index(parser: &mut Parser) -> Result<Vec<StatementIndex>> {
    debug!("parsing index");

    parser.next();
    let mut indices = vec![];

    while let Some(t) = parser.current() {
        debug!(?t, "parsing token");
        match t {
            Token::Seperator(Seperator::Comma) => {
                parser.next();
                continue;
            }
            Token::Ident(ident) => {
                let column = ident.to_owned();
                parser.next();
                let operator = parse_operator(parser)?;
                parser.next();
                let expr = parse_expression_statement(parser)
                    .ok_or_else(|| ParseError::ParseError("couldnt parse expression"))?
                    .evaluate()?;

                let index = StatementIndex {
                    column,
                    operator,
                    expr,
                };
                indices.push(index);
                parser.next();
            }

            Token::Eof | Token::Seperator(Seperator::Semicolon) | Token::Keyword(_) => break,

            t => {
                return Err(ParseError::InvalidToken {
                    expected: "expected identifier, seperator or EOF".to_string(),
                    got: t.to_string(),
                }
                .into());
            }
        }
    }

    Ok(indices)
}

pub fn parse_operator(parser: &mut Parser) -> Result<Operator> {
    debug!("parsing operator");

    if let Some(t) = parser.current() {
        match t {
            Token::Operator(Operator::Assign) => Ok(Operator::Equal),
            Token::Operator(Operator::Equal) => Ok(Operator::Equal),
            Token::Operator(Operator::Ge) => Ok(Operator::Ge),
            Token::Operator(Operator::Gt) => Ok(Operator::Gt),
            Token::Operator(Operator::Le) => Ok(Operator::Le),
            Token::Operator(Operator::Lt) => Ok(Operator::Lt),

            t => Err(ParseError::InvalidToken {
                expected: "comparison operator".to_string(),
                got: t.to_string(),
            }
            .into()),
        }
    } else {
        Err(ParseError::ParseError("missing token").into())
    }
}

pub fn parse_limit(parser: &mut Parser) -> Result<StatementLimit> {
    info!("parsing LIMIT clause");

    parser.next();
    let expr = parse_expression_statement(parser)
        .ok_or_else(|| ParseError::ParseError("parsing LIMIT clause failed"))?
        .evaluate()?;
    parser.next();
    match expr {
        ValueObject::Str(_) => {
            return Err(ParseError::ParseError("cant use strings for limit clause").into());
        }
        ValueObject::Int(i) => Ok(StatementLimit(i)),
    }
}

pub fn parse_order(parser: &mut Parser) -> Result<StatementOrder> {
    info!("parsing ORDER clause");

    parser.next();
    let column = parse_identifier(parser)?;
    Ok(StatementOrder { column })
}

pub fn parse_expression_statement(parser: &mut Parser) -> Option<Box<dyn Expression>> {
    info!(?parser.lexer.current, ?parser.lexer.next, "parsing expression statement");

    // parser.next();
    parser.parse_expression(Precedence::Lowest)
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
    debug!(?parser.lexer.current, "parsing rhs");
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
    fn expression_test1() {
        let input = "10 + 10";
        let mut parser = Parser::new(input);
        parser.next();
        let expr = parse_expression_statement(&mut parser)
            .unwrap()
            .evaluate()
            .unwrap();

        assert_eq!(expr, ValueObject::Int(20));

        let input = "(-(10 - 5) * 2)";
        let mut parser = Parser::new(input);
        parser.next();
        let expr = parse_expression_statement(&mut parser)
            .unwrap()
            .evaluate()
            .unwrap();

        assert_eq!(expr, ValueObject::Int(-10));

        let input = "\"hello\" + \"world\"";
        let mut parser = Parser::new(input);
        parser.next();
        let expr = parse_expression_statement(&mut parser)
            .unwrap()
            .evaluate()
            .unwrap();

        assert_eq!(expr, ValueObject::Str("helloworld".into()));
    }
}
