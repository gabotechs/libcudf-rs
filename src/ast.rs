use crate::{CuDFError, CuDFScalar};
use arrow::error::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::{ffi, AstOperator, AstTableReference, TypeId};
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
        let dtype = scalar.inner().data_type()?.id();
        let tree = self.inner.pin_mut();
        let inner = scalar.inner();
        // SAFETY: the selected wrapper matches the scalar type id read from the
        // same cuDF scalar, and `self.literals` retains its allocation for the tree.
        // The C++ wrapper validates the type again before casting.
        let node = unsafe {
            match dtype {
                x if x == TypeId::Int8 as i32 => {
                    ffi::ast_expression_tree_add_literal_int8(tree, inner)
                }
                x if x == TypeId::Int16 as i32 => {
                    ffi::ast_expression_tree_add_literal_int16(tree, inner)
                }
                x if x == TypeId::Int32 as i32 => {
                    ffi::ast_expression_tree_add_literal_int32(tree, inner)
                }
                x if x == TypeId::Int64 as i32 => {
                    ffi::ast_expression_tree_add_literal_int64(tree, inner)
                }
                x if x == TypeId::Uint8 as i32 => {
                    ffi::ast_expression_tree_add_literal_uint8(tree, inner)
                }
                x if x == TypeId::Uint16 as i32 => {
                    ffi::ast_expression_tree_add_literal_uint16(tree, inner)
                }
                x if x == TypeId::Uint32 as i32 => {
                    ffi::ast_expression_tree_add_literal_uint32(tree, inner)
                }
                x if x == TypeId::Uint64 as i32 => {
                    ffi::ast_expression_tree_add_literal_uint64(tree, inner)
                }
                x if x == TypeId::Float32 as i32 => {
                    ffi::ast_expression_tree_add_literal_float32(tree, inner)
                }
                x if x == TypeId::Float64 as i32 => {
                    ffi::ast_expression_tree_add_literal_float64(tree, inner)
                }
                x if x == TypeId::Bool8 as i32 => {
                    ffi::ast_expression_tree_add_literal_bool8(tree, inner)
                }
                x if x == TypeId::String as i32 => {
                    ffi::ast_expression_tree_add_literal_string(tree, inner)
                }
                x if x == TypeId::TimestampDays as i32 => {
                    ffi::ast_expression_tree_add_literal_timestamp_days(tree, inner)
                }
                x if x == TypeId::TimestampSeconds as i32 => {
                    ffi::ast_expression_tree_add_literal_timestamp_seconds(tree, inner)
                }
                x if x == TypeId::TimestampMilliseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_timestamp_milliseconds(tree, inner)
                }
                x if x == TypeId::TimestampMicroseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_timestamp_microseconds(tree, inner)
                }
                x if x == TypeId::TimestampNanoseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_timestamp_nanoseconds(tree, inner)
                }
                x if x == TypeId::DurationDays as i32 => {
                    ffi::ast_expression_tree_add_literal_duration_days(tree, inner)
                }
                x if x == TypeId::DurationSeconds as i32 => {
                    ffi::ast_expression_tree_add_literal_duration_seconds(tree, inner)
                }
                x if x == TypeId::DurationMilliseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_duration_milliseconds(tree, inner)
                }
                x if x == TypeId::DurationMicroseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_duration_microseconds(tree, inner)
                }
                x if x == TypeId::DurationNanoseconds as i32 => {
                    ffi::ast_expression_tree_add_literal_duration_nanoseconds(tree, inner)
                }
                x if x == TypeId::Decimal32 as i32 => {
                    ffi::ast_expression_tree_add_literal_decimal32(tree, inner)
                }
                x if x == TypeId::Decimal64 as i32 => {
                    ffi::ast_expression_tree_add_literal_decimal64(tree, inner)
                }
                x if x == TypeId::Decimal128 as i32 => {
                    ffi::ast_expression_tree_add_literal_decimal128(tree, inner)
                }
                _ => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "cuDF AST literals do not support type id {dtype}"
                    ))
                    .into())
                }
            }
        }?;
        self.literals.push(scalar);
        Ok(node)
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
