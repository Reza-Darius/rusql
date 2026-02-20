use crate::{
    database::errors::*,
    interpreter::{parser::types::ValueObject, tokens::Operator},
};
use std::fmt::Display;

pub trait Expression: std::fmt::Debug {
    fn evaluate(&self) -> Result<ValueObject>;
}

impl From<&str> for Box<dyn Expression> {
    fn from(value: &str) -> Self {
        Box::new(StrLiteral(value.to_owned()))
    }
}

impl From<i64> for Box<dyn Expression> {
    fn from(value: i64) -> Self {
        Box::new(IntLiteral(format!("{value}")))
    }
}

#[derive(Debug)]
pub struct IntLiteral(pub String);

impl Expression for IntLiteral {
    fn evaluate(&self) -> Result<ValueObject> {
        self.0
            .parse::<i64>()
            .map(|i| ValueObject::Int(i))
            .map_err(|e| ParseError::ParseError("couldnt parse int literal").into())
    }
}

#[derive(Debug)]
pub struct StrLiteral(pub String);

impl Expression for StrLiteral {
    fn evaluate(&self) -> Result<ValueObject> {
        assert!(!self.0.is_empty());
        Ok(ValueObject::Str(self.0.clone()))
    }
}

#[derive(Debug)]
pub struct InfixExpression {
    pub lhs: Option<Box<dyn Expression>>,
    pub operator: Operator,
    pub rhs: Option<Box<dyn Expression>>,
}

impl Expression for InfixExpression {
    fn evaluate(&self) -> Result<ValueObject> {
        let lhs = self
            .lhs
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("no lhs expression found!"))?
            .evaluate()?;

        let rhs = self
            .rhs
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("no rhs expression found!"))?
            .evaluate()?;

        match &lhs {
            ValueObject::Str(sl) => match &rhs {
                ValueObject::Str(sr) => eval_with_str(sl, sr, self.operator),
                ValueObject::Int(ir) => eval_with_str(sl, ir, self.operator),
            },
            ValueObject::Int(il) => match &rhs {
                ValueObject::Str(sr) => eval_with_str(il, sr, self.operator),
                ValueObject::Int(ir) => eval_int_int(*il, *ir, self.operator),
            },
        }
    }
}

fn eval_with_str(a: &impl Display, b: &impl Display, op: Operator) -> Result<ValueObject> {
    match op {
        Operator::Plus => Ok(ValueObject::Str(format!("{a}{b}"))),
        _ => Err(ParseError::ParseError("invalid operator for str infix expression").into()),
    }
}

fn eval_int_int(int_a: i64, int_b: i64, op: Operator) -> Result<ValueObject> {
    match op {
        Operator::Plus => Ok(ValueObject::Int(int_a + int_b)),
        Operator::Minus => Ok(ValueObject::Int(int_a - int_b)),
        Operator::Multi => Ok(ValueObject::Int(int_a * int_b)),
        Operator::Divide => Ok(ValueObject::Int(int_a / int_b)),
        Operator::Modulo => Ok(ValueObject::Int(int_a % int_b)),
        _ => Err(ParseError::ParseError("invalid operator for int to int infix expression").into()),
    }
}

#[derive(Debug)]
pub struct PrefixExpression {
    pub operator: Operator,
    pub rhs: Option<Box<dyn Expression>>,
}

impl Expression for PrefixExpression {
    fn evaluate(&self) -> Result<ValueObject> {
        let expr = self
            .rhs
            .as_ref()
            .ok_or_else(|| ParseError::ParseError("no expression found!"))?
            .evaluate()?;

        match expr {
            ValueObject::Str(_) => Err(ParseError::ParseError(
                "invalid expression: cant have a string for a prefix expression",
            ))?,
            ValueObject::Int(i) => match self.operator {
                Operator::Minus => Ok(ValueObject::Int(-i)),
                _ => Err(ParseError::ParseError("invalid prefix operator"))?,
            },
        }
    }
}

#[cfg(test)]
mod eval_test {
    use super::*;
    use test_log::test;

    #[test]
    fn eval_test1() {
        let mut expr = InfixExpression {
            lhs: Some(10.into()),
            operator: Operator::Plus,
            rhs: Some(10.into()),
        };

        assert_eq!(expr.evaluate().unwrap(), ValueObject::Int(20));

        expr = InfixExpression {
            lhs: Some("Hello".into()),
            operator: Operator::Plus,
            rhs: Some("World".into()),
        };

        assert_eq!(
            expr.evaluate().unwrap(),
            ValueObject::Str("HelloWorld".to_string())
        );

        expr = InfixExpression {
            lhs: Some(10.into()),
            operator: Operator::Plus,
            rhs: Some("World".into()),
        };

        assert_eq!(
            expr.evaluate().unwrap(),
            ValueObject::Str("10World".to_string())
        );

        let mut expr = PrefixExpression {
            operator: Operator::Minus,
            rhs: Some(10.into()),
        };

        assert_eq!(expr.evaluate().unwrap(), ValueObject::Int(-10));

        expr = PrefixExpression {
            operator: Operator::Modulo,
            rhs: Some(10.into()),
        };
        assert!(expr.evaluate().is_err())
    }
}
