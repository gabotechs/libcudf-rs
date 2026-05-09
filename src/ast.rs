use crate::{CuDFError, CuDFScalar};
use arrow::error::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::{ffi, AstOperator, AstTableReference};
use std::fmt;

/// A node index inside a [`CuDFAstExpression`] tree.
pub type CuDFAstNode = usize;

/// Table side used by a cuDF AST column reference.
///
/// Join predicates evaluate against two input tables. `Left` and `Right`
/// select which table a column reference reads from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CuDFAstTableReference {
    /// Column index in the left table.
    Left,
    /// Column index in the right table.
    Right,
    /// Column index in the output table.
    Output,
}

impl CuDFAstTableReference {
    fn into_sys(self) -> AstTableReference {
        match self {
            Self::Left => AstTableReference::Left,
            Self::Right => AstTableReference::Right,
            Self::Output => AstTableReference::Output,
        }
    }
}

/// Operators supported by [`CuDFAstExpression`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CuDFAstOperator {
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
    /// Equality comparison.
    Equal,
    /// Non-equality comparison.
    NotEqual,
    /// Less-than comparison.
    Less,
    /// Greater-than comparison.
    Greater,
    /// Less-than-or-equal comparison.
    LessEqual,
    /// Greater-than-or-equal comparison.
    GreaterEqual,
    /// Logical AND.
    LogicalAnd,
    /// Null-aware logical AND.
    NullLogicalAnd,
    /// Logical OR.
    LogicalOr,
    /// Null-aware logical OR.
    NullLogicalOr,
    /// Modulo.
    Mod,
    /// Null-aware equality comparison.
    NullEqual,
    /// Null check.
    IsNull,
    /// Logical NOT.
    Not,
    /// Cast to int64.
    CastToInt64,
    /// Cast to uint64.
    CastToUint64,
    /// Cast to float64.
    CastToFloat64,
}

impl CuDFAstOperator {
    fn into_sys(self) -> AstOperator {
        match self {
            Self::Add => AstOperator::Add,
            Self::Sub => AstOperator::Sub,
            Self::Mul => AstOperator::Mul,
            Self::Div => AstOperator::Div,
            Self::Equal => AstOperator::Equal,
            Self::NotEqual => AstOperator::NotEqual,
            Self::Less => AstOperator::Less,
            Self::Greater => AstOperator::Greater,
            Self::LessEqual => AstOperator::LessEqual,
            Self::GreaterEqual => AstOperator::GreaterEqual,
            Self::LogicalAnd => AstOperator::LogicalAnd,
            Self::NullLogicalAnd => AstOperator::NullLogicalAnd,
            Self::LogicalOr => AstOperator::LogicalOr,
            Self::NullLogicalOr => AstOperator::NullLogicalOr,
            Self::Mod => AstOperator::Mod,
            Self::NullEqual => AstOperator::NullEqual,
            Self::IsNull => AstOperator::IsNull,
            Self::Not => AstOperator::Not,
            Self::CastToInt64 => AstOperator::CastToInt64,
            Self::CastToUint64 => AstOperator::CastToUint64,
            Self::CastToFloat64 => AstOperator::CastToFloat64,
        }
    }
}

/// Owning cuDF AST expression tree.
///
/// Literal scalars are kept alive by this wrapper because cuDF AST literal
/// nodes reference scalar objects owned outside the tree.
///
/// The most recently added node is the expression root used by join filtering.
pub struct CuDFAstExpression {
    inner: UniquePtr<ffi::AstExpressionTree>,
    literals: Vec<CuDFScalar>,
}

impl fmt::Debug for CuDFAstExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CuDFAstExpression").finish_non_exhaustive()
    }
}

impl CuDFAstExpression {
    /// Create an empty cuDF AST expression tree.
    pub fn new() -> Self {
        Self {
            inner: ffi::ast_expression_tree_create(),
            literals: Vec::new(),
        }
    }

    /// Add a column reference node.
    ///
    /// `column_index` is relative to the table selected by `table`. For join
    /// filters this is usually a projected conditional table, not necessarily
    /// the full input table.
    ///
    /// # Errors
    ///
    /// Returns an error if `column_index` cannot be represented as a cuDF
    /// column index.
    pub fn column_reference(
        &mut self,
        column_index: usize,
        table: CuDFAstTableReference,
    ) -> Result<CuDFAstNode, CuDFError> {
        Ok(ffi::ast_expression_tree_add_column_reference(
            self.inner.pin_mut(),
            column_index.try_into().map_err(|_| {
                CuDFError::ArrowError(ArrowError::InvalidArgumentError(format!(
                    "AST column index {column_index} exceeds i32"
                )))
            })?,
            table.into_sys() as i32,
        )?)
    }

    /// Add a column-name reference node.
    ///
    /// cuDF Parquet filters use names so filter columns do not need to be
    /// present in the projected output.
    pub fn column_name_reference(
        &mut self,
        column_name: impl AsRef<str>,
    ) -> Result<CuDFAstNode, CuDFError> {
        Ok(ffi::ast_expression_tree_add_column_name_reference(
            self.inner.pin_mut(),
            column_name.as_ref(),
        )?)
    }

    /// Add a literal node.
    ///
    /// The scalar is moved into this expression tree wrapper and kept alive for
    /// as long as the AST exists.
    ///
    /// # Errors
    ///
    /// Returns an error if cuDF cannot add the scalar as an AST literal.
    pub fn literal(&mut self, scalar: CuDFScalar) -> Result<CuDFAstNode, CuDFError> {
        self.literals.push(scalar);
        let scalar = self
            .literals
            .last()
            .expect("literal scalar was just inserted");
        Ok(ffi::ast_expression_tree_add_literal(
            self.inner.pin_mut(),
            scalar.inner(),
        )?)
    }

    /// Add a unary operation node.
    ///
    /// # Errors
    ///
    /// Returns an error if `input` does not refer to an existing AST node or
    /// cuDF rejects the requested operation.
    pub fn unary_operation(
        &mut self,
        op: CuDFAstOperator,
        input: CuDFAstNode,
    ) -> Result<CuDFAstNode, CuDFError> {
        Ok(ffi::ast_expression_tree_add_unary_operation(
            self.inner.pin_mut(),
            op.into_sys() as i32,
            input,
        )?)
    }

    /// Add a binary operation node.
    ///
    /// # Errors
    ///
    /// Returns an error if either operand does not refer to an existing AST
    /// node or cuDF rejects the requested operation.
    pub fn binary_operation(
        &mut self,
        op: CuDFAstOperator,
        left: CuDFAstNode,
        right: CuDFAstNode,
    ) -> Result<CuDFAstNode, CuDFError> {
        Ok(ffi::ast_expression_tree_add_operation(
            self.inner.pin_mut(),
            op.into_sys() as i32,
            left,
            right,
        )?)
    }

    pub(crate) fn inner(&self) -> &UniquePtr<ffi::AstExpressionTree> {
        &self.inner
    }
}

impl Default for CuDFAstExpression {
    fn default() -> Self {
        Self::new()
    }
}
