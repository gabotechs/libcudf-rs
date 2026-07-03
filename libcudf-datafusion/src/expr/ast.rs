use crate::errors::cudf_to_df;
use crate::execution::execute_cudf;
use crate::physical::normalize_scalar_for_cudf;
use arrow_schema::{DataType, Schema};
use datafusion::common::{internal_err, not_impl_err, JoinSide};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::Operator;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use libcudf_rs::{
    CuDFAstExpression, CuDFAstExpressionBuilder, CuDFAstNode, CuDFAstOperator,
    CuDFAstTableReference, CuDFScalar,
};

/// cuDF AST filter plus the file-schema columns it reads.
pub(crate) struct CuDFParquetScanFilter {
    /// Predicate expression passed to the cuDF Parquet reader.
    pub(crate) expression: CuDFAstExpression,
    /// File column names referenced by the predicate.
    pub(crate) column_names: Vec<String>,
}

/// Lower a DataFusion join filter into a cuDF AST predicate.
///
/// DataFusion filter columns reference the filter's intermediate schema. This
/// rewrites them to cuDF left/right table references using `ColumnIndex`; for
/// example, filter column `0` with `ColumnIndex { index: 2, side: Left }`
/// becomes `left.column(2)` in the cuDF predicate.
pub(crate) fn join_filter_to_cudf_ast(
    filter: &JoinFilter,
) -> Result<CuDFAstExpression, DataFusionError> {
    if filter.expression().data_type(filter.schema().as_ref())? != DataType::Boolean {
        return not_impl_err!("Join filter expression must evaluate to Boolean for cuDF AST");
    }

    let mut ast = CuDFAstExpression::builder();
    let mut resolver = JoinColumnResolver {
        column_indices: filter.column_indices(),
    };
    lower_expr(filter.expression().as_ref(), &mut resolver, &mut ast)?;
    Ok(ast.finish())
}

pub(crate) fn is_join_filter_supported_by_cudf_ast(
    filter: &JoinFilter,
) -> Result<bool, DataFusionError> {
    if filter.expression().data_type(filter.schema().as_ref())? != DataType::Boolean {
        return Ok(false);
    }

    Ok(can_lower_expr(
        filter.expression().as_ref(),
        &JoinColumnResolver {
            column_indices: filter.column_indices(),
        },
    ))
}

/// Lower a DataFusion Parquet scan predicate into a cuDF AST predicate.
///
/// The predicate uses cuDF column-name references so filter columns do not need
/// to be present in the scan projection.
pub(crate) fn parquet_filter_to_cudf_filter(
    filter: &dyn PhysicalExpr,
    file_schema: &Schema,
) -> Result<CuDFParquetScanFilter, DataFusionError> {
    if filter.data_type(file_schema)? != DataType::Boolean {
        return not_impl_err!("Parquet filter expression must evaluate to Boolean for cuDF AST");
    }

    let mut ast = CuDFAstExpression::builder();
    let mut resolver = ParquetColumnResolver::new(file_schema);
    lower_expr(filter, &mut resolver, &mut ast)?;

    Ok(CuDFParquetScanFilter {
        expression: ast.finish(),
        column_names: resolver.into_column_names(),
    })
}

trait AstColumnResolver {
    fn context(&self) -> &'static str;

    fn lower_column(
        &mut self,
        column: &Column,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>;

    fn can_lower_column(&self, column: &Column) -> bool;

    fn lower_binary(
        &mut self,
        expr: &BinaryExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>
    where
        Self: Sized,
    {
        lower_binary_expr(expr, self, ast)
    }

    fn can_lower_binary(&self, expr: &BinaryExpr) -> bool
    where
        Self: Sized,
    {
        can_lower_binary_expr(expr, self)
    }

    fn lower_cast(
        &mut self,
        cast: &CastExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>
    where
        Self: Sized,
    {
        lower_cast_expr(cast, self, ast)
    }

    fn can_lower_cast(&self, cast: &CastExpr) -> bool
    where
        Self: Sized,
    {
        matches!(
            cast.cast_type(),
            DataType::Int64 | DataType::UInt64 | DataType::Float64
        ) && can_lower_expr(cast.expr().as_ref(), self)
    }

    fn lower_not(
        &mut self,
        not: &NotExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>
    where
        Self: Sized,
    {
        let input = lower_expr(not.arg().as_ref(), self, ast)?;
        ast.unary_operation(CuDFAstOperator::Not, input)
            .map_err(cudf_to_df)
    }

    fn can_lower_not(&self, not: &NotExpr) -> bool
    where
        Self: Sized,
    {
        can_lower_expr(not.arg().as_ref(), self)
    }

    fn lower_is_null(
        &mut self,
        is_null: &IsNullExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>
    where
        Self: Sized,
    {
        let input = lower_expr(is_null.arg().as_ref(), self, ast)?;
        ast.unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df)
    }

    fn can_lower_is_null(&self, is_null: &IsNullExpr) -> bool
    where
        Self: Sized,
    {
        can_lower_expr(is_null.arg().as_ref(), self)
    }

    fn lower_is_not_null(
        &mut self,
        is_not_null: &IsNotNullExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError>
    where
        Self: Sized,
    {
        let input = lower_expr(is_not_null.arg().as_ref(), self, ast)?;
        let is_null = ast
            .unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df)?;
        ast.unary_operation(CuDFAstOperator::Not, is_null)
            .map_err(cudf_to_df)
    }

    fn can_lower_is_not_null(&self, is_not_null: &IsNotNullExpr) -> bool
    where
        Self: Sized,
    {
        can_lower_expr(is_not_null.arg().as_ref(), self)
    }
}

struct JoinColumnResolver<'a> {
    column_indices: &'a [ColumnIndex],
}

impl AstColumnResolver for JoinColumnResolver<'_> {
    fn context(&self) -> &'static str {
        "Join filter"
    }

    fn lower_column(
        &mut self,
        column: &Column,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        let Some(ColumnIndex { index, side }) = self.column_indices.get(column.index()) else {
            return not_impl_err!(
                "Join filter column {} is outside the filter column index list",
                column.index()
            );
        };
        let table = match side {
            JoinSide::Left => CuDFAstTableReference::Left,
            JoinSide::Right => CuDFAstTableReference::Right,
            JoinSide::None => {
                return not_impl_err!("Join filter marker columns are not supported by cuDF AST");
            }
        };
        ast.column_reference(*index, table).map_err(cudf_to_df)
    }

    fn can_lower_column(&self, column: &Column) -> bool {
        self.column_indices
            .get(column.index())
            .is_some_and(|column_index| column_index.side != JoinSide::None)
    }
}

struct ParquetColumnResolver<'a> {
    file_schema: &'a Schema,
    column_names: Vec<String>,
}

impl<'a> ParquetColumnResolver<'a> {
    fn new(file_schema: &'a Schema) -> Self {
        Self {
            file_schema,
            column_names: Vec::new(),
        }
    }

    fn into_column_names(self) -> Vec<String> {
        self.column_names
    }

    fn lower_column_arg(
        &mut self,
        expr: &dyn PhysicalExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        let Some(column) = expr.as_any().downcast_ref::<Column>() else {
            return unsupported_parquet_filter();
        };
        self.lower_column(column, ast)
    }
}

impl AstColumnResolver for ParquetColumnResolver<'_> {
    fn context(&self) -> &'static str {
        "Parquet filter"
    }

    fn lower_column(
        &mut self,
        column: &Column,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        let Some((name, data_type)) = parquet_column_name_and_type(column, self.file_schema) else {
            return unsupported_parquet_filter();
        };
        if !is_supported_parquet_filter_type(&data_type) {
            return unsupported_parquet_filter();
        }
        if !self.column_names.contains(&name) {
            self.column_names.push(name.clone());
        }
        ast.column_name_reference(name).map_err(cudf_to_df)
    }

    fn can_lower_column(&self, column: &Column) -> bool {
        parquet_column_name_and_type(column, self.file_schema)
            .is_some_and(|(_, data_type)| is_supported_parquet_filter_type(&data_type))
    }

    fn lower_binary(
        &mut self,
        expr: &BinaryExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        match expr.op() {
            Operator::And | Operator::Or => {
                let left = lower_expr(expr.left().as_ref(), self, ast)?;
                let right = lower_expr(expr.right().as_ref(), self, ast)?;
                let Some(op) = map_binary_op(expr.op()) else {
                    return internal_err!("logical parquet operator should map to cuDF AST");
                };
                ast.binary_operation(op, left, right).map_err(cudf_to_df)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                validate_parquet_comparison(
                    expr.left().as_ref(),
                    expr.right().as_ref(),
                    self.file_schema,
                )?;
                let left = lower_expr(expr.left().as_ref(), self, ast)?;
                let right = lower_expr(expr.right().as_ref(), self, ast)?;
                let Some(op) = map_binary_op(expr.op()) else {
                    return internal_err!("comparison parquet operator should map to cuDF AST");
                };
                ast.binary_operation(op, left, right).map_err(cudf_to_df)
            }
            _ => unsupported_parquet_filter(),
        }
    }

    fn can_lower_binary(&self, expr: &BinaryExpr) -> bool {
        match expr.op() {
            Operator::And | Operator::Or => {
                can_lower_expr(expr.left().as_ref(), self)
                    && can_lower_expr(expr.right().as_ref(), self)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                is_supported_parquet_comparison(
                    expr.left().as_ref(),
                    expr.right().as_ref(),
                    self.file_schema,
                ) && can_lower_expr(expr.left().as_ref(), self)
                    && can_lower_expr(expr.right().as_ref(), self)
            }
            _ => false,
        }
    }

    fn lower_cast(
        &mut self,
        _cast: &CastExpr,
        _ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        unsupported_parquet_filter()
    }

    fn can_lower_cast(&self, _cast: &CastExpr) -> bool {
        false
    }

    fn lower_not(
        &mut self,
        _not: &NotExpr,
        _ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        unsupported_parquet_filter()
    }

    fn can_lower_not(&self, _not: &NotExpr) -> bool {
        false
    }

    fn lower_is_null(
        &mut self,
        is_null: &IsNullExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        let input = self.lower_column_arg(is_null.arg().as_ref(), ast)?;
        ast.unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df)
    }

    fn can_lower_is_null(&self, is_null: &IsNullExpr) -> bool {
        is_null
            .arg()
            .as_any()
            .downcast_ref::<Column>()
            .is_some_and(|column| self.can_lower_column(column))
    }

    fn lower_is_not_null(
        &mut self,
        is_not_null: &IsNotNullExpr,
        ast: &mut CuDFAstExpressionBuilder,
    ) -> Result<CuDFAstNode, DataFusionError> {
        let input = self.lower_column_arg(is_not_null.arg().as_ref(), ast)?;
        let is_null = ast
            .unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df)?;
        ast.unary_operation(CuDFAstOperator::Not, is_null)
            .map_err(cudf_to_df)
    }

    fn can_lower_is_not_null(&self, is_not_null: &IsNotNullExpr) -> bool {
        is_not_null
            .arg()
            .as_any()
            .downcast_ref::<Column>()
            .is_some_and(|column| self.can_lower_column(column))
    }
}

fn lower_expr(
    expr: &dyn PhysicalExpr,
    resolver: &mut impl AstColumnResolver,
    ast: &mut CuDFAstExpressionBuilder,
) -> Result<CuDFAstNode, DataFusionError> {
    let any = expr.as_any();
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return resolver.lower_binary(binary, ast);
    }
    if let Some(column) = any.downcast_ref::<Column>() {
        return resolver.lower_column(column, ast);
    }
    if let Some(literal) = any.downcast_ref::<Literal>() {
        return lower_literal_expr(literal, ast);
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return resolver.lower_cast(cast, ast);
    }
    if let Some(not) = any.downcast_ref::<NotExpr>() {
        return resolver.lower_not(not, ast);
    }
    if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
        return resolver.lower_is_null(is_null, ast);
    }
    if let Some(is_not_null) = any.downcast_ref::<IsNotNullExpr>() {
        return resolver.lower_is_not_null(is_not_null, ast);
    }

    not_impl_err!(
        "{} expression {expr} is not supported by cuDF AST",
        resolver.context()
    )
}

fn can_lower_expr(expr: &dyn PhysicalExpr, resolver: &impl AstColumnResolver) -> bool {
    let any = expr.as_any();
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return resolver.can_lower_binary(binary);
    }
    if let Some(column) = any.downcast_ref::<Column>() {
        return resolver.can_lower_column(column);
    }
    if let Some(literal) = any.downcast_ref::<Literal>() {
        return normalize_scalar_for_cudf(literal.value().clone()).is_ok();
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return resolver.can_lower_cast(cast);
    }
    if let Some(not) = any.downcast_ref::<NotExpr>() {
        return resolver.can_lower_not(not);
    }
    if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
        return resolver.can_lower_is_null(is_null);
    }
    if let Some(is_not_null) = any.downcast_ref::<IsNotNullExpr>() {
        return resolver.can_lower_is_not_null(is_not_null);
    }

    false
}

fn validate_parquet_comparison(
    left: &dyn PhysicalExpr,
    right: &dyn PhysicalExpr,
    file_schema: &Schema,
) -> Result<(), DataFusionError> {
    if is_supported_parquet_comparison(left, right, file_schema) {
        Ok(())
    } else {
        unsupported_parquet_filter()
    }
}

fn is_supported_parquet_comparison(
    left: &dyn PhysicalExpr,
    right: &dyn PhysicalExpr,
    file_schema: &Schema,
) -> bool {
    let left_is_column = parquet_column_name_and_type_expr(left, file_schema).is_some();
    let right_is_column = parquet_column_name_and_type_expr(right, file_schema).is_some();
    let left_literal_type = parquet_literal_type(left, file_schema);
    let right_literal_type = parquet_literal_type(right, file_schema);

    match (
        left_is_column,
        right_is_column,
        left_literal_type,
        right_literal_type,
    ) {
        (true, false, _, Some(data_type)) | (false, true, Some(data_type), _) => {
            is_supported_parquet_filter_type(&data_type)
        }
        _ => false,
    }
}

fn parquet_literal_type(expr: &dyn PhysicalExpr, file_schema: &Schema) -> Option<DataType> {
    let literal = expr.as_any().downcast_ref::<Literal>()?;
    normalize_scalar_for_cudf(literal.value().clone()).ok()?;
    literal.data_type(file_schema).ok()
}

fn parquet_column_name_and_type_expr(
    expr: &dyn PhysicalExpr,
    file_schema: &Schema,
) -> Option<(String, DataType)> {
    let column = expr.as_any().downcast_ref::<Column>()?;
    parquet_column_name_and_type(column, file_schema)
}

fn parquet_column_name_and_type(
    column: &Column,
    file_schema: &Schema,
) -> Option<(String, DataType)> {
    let field = file_schema.fields().get(column.index())?;
    if column.name() != field.name() {
        return None;
    }
    Some((field.name().clone(), column.data_type(file_schema).ok()?))
}

fn is_supported_parquet_filter_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::BinaryView
    )
}

fn unsupported_parquet_filter<T>() -> Result<T, DataFusionError> {
    not_impl_err!("Parquet filter expression is not supported by cuDF Parquet scan")
}

fn lower_binary_expr(
    expr: &BinaryExpr,
    resolver: &mut impl AstColumnResolver,
    ast: &mut CuDFAstExpressionBuilder,
) -> Result<CuDFAstNode, DataFusionError> {
    let left = lower_expr(expr.left().as_ref(), resolver, ast)?;
    let right = lower_expr(expr.right().as_ref(), resolver, ast)?;
    let op = match expr.op() {
        Operator::IsNotDistinctFrom => CuDFAstOperator::NullEqual,
        Operator::IsDistinctFrom => {
            let is_not_distinct = ast
                .binary_operation(CuDFAstOperator::NullEqual, left, right)
                .map_err(cudf_to_df)?;
            return ast
                .unary_operation(CuDFAstOperator::Not, is_not_distinct)
                .map_err(cudf_to_df);
        }
        other => map_binary_op(other).ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "{} operator {other:?} is not supported by cuDF AST",
                resolver.context()
            ))
        })?,
    };

    ast.binary_operation(op, left, right).map_err(cudf_to_df)
}

fn can_lower_binary_expr(expr: &BinaryExpr, resolver: &impl AstColumnResolver) -> bool {
    can_lower_expr(expr.left().as_ref(), resolver)
        && can_lower_expr(expr.right().as_ref(), resolver)
        && (map_binary_op(expr.op()).is_some()
            || matches!(
                expr.op(),
                Operator::IsDistinctFrom | Operator::IsNotDistinctFrom
            ))
}

fn lower_literal_expr(
    literal: &Literal,
    ast: &mut CuDFAstExpressionBuilder,
) -> Result<CuDFAstNode, DataFusionError> {
    let value = normalize_scalar_for_cudf(literal.value().clone())?;
    let scalar = execute_cudf(CuDFScalar::from_arrow_host(value.to_scalar()?))?;
    ast.literal(scalar).map_err(cudf_to_df)
}

fn lower_cast_expr(
    cast: &CastExpr,
    resolver: &mut impl AstColumnResolver,
    ast: &mut CuDFAstExpressionBuilder,
) -> Result<CuDFAstNode, DataFusionError> {
    let input = lower_expr(cast.expr().as_ref(), resolver, ast)?;
    let op = match cast.cast_type() {
        DataType::Int64 => CuDFAstOperator::CastToInt64,
        DataType::UInt64 => CuDFAstOperator::CastToUint64,
        DataType::Float64 => CuDFAstOperator::CastToFloat64,
        other => {
            return not_impl_err!(
                "{} cast to {other} is not supported by cuDF AST",
                resolver.context()
            );
        }
    };
    ast.unary_operation(op, input).map_err(cudf_to_df)
}

fn map_binary_op(op: &Operator) -> Option<CuDFAstOperator> {
    match op {
        Operator::Eq => Some(CuDFAstOperator::Equal),
        Operator::NotEq => Some(CuDFAstOperator::NotEqual),
        Operator::Lt => Some(CuDFAstOperator::Less),
        Operator::LtEq => Some(CuDFAstOperator::LessEqual),
        Operator::Gt => Some(CuDFAstOperator::Greater),
        Operator::GtEq => Some(CuDFAstOperator::GreaterEqual),
        Operator::Plus => Some(CuDFAstOperator::Add),
        Operator::Minus => Some(CuDFAstOperator::Sub),
        Operator::Multiply => Some(CuDFAstOperator::Mul),
        Operator::Divide => Some(CuDFAstOperator::Div),
        Operator::Modulo => Some(CuDFAstOperator::Mod),
        Operator::And => Some(CuDFAstOperator::NullLogicalAnd),
        Operator::Or => Some(CuDFAstOperator::NullLogicalOr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_join_filter_supported_by_cudf_ast, join_filter_to_cudf_ast,
        parquet_filter_to_cudf_filter,
    };
    use crate::execution::execute_cudf;
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::{DataFusionError, JoinSide, ScalarValue};
    use datafusion::physical_expr::expressions::{
        in_list, BinaryExpr, Column, IsNullExpr, Literal,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion_expr::Operator;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use libcudf_rs::{CuDFHashJoin, CuDFNullEquality, CuDFTable};
    use std::error::Error;
    use std::sync::Arc;

    #[test]
    fn test_join_filter_ast_maps_intermediate_columns_to_join_sides() -> Result<(), Box<dyn Error>>
    {
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left_val", 0)),
                Operator::Lt,
                Arc::new(Column::new("right_val", 1)),
            )) as Arc<dyn PhysicalExpr>,
            filter_indices(),
            filter_schema(),
        );

        let batch = run_filtered_inner_join(filter)?;
        assert_eq!(int32_values(&batch, 0), vec![20]);
        assert_eq!(int32_values(&batch, 1), vec![25]);
        Ok(())
    }

    #[test]
    fn test_join_filter_ast_lowers_literals_and_logical_ops() -> Result<(), Box<dyn Error>> {
        let left_plus_five = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("left_val", 0)),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        )) as Arc<dyn PhysicalExpr>;
        let first = Arc::new(BinaryExpr::new(
            left_plus_five,
            Operator::LtEq,
            Arc::new(Column::new("right_val", 1)),
        )) as Arc<dyn PhysicalExpr>;
        let second = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("right_val", 1)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(25)))),
        )) as Arc<dyn PhysicalExpr>;
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(first, Operator::And, second)) as Arc<dyn PhysicalExpr>,
            filter_indices(),
            filter_schema(),
        );

        let batch = run_filtered_inner_join(filter)?;
        assert_eq!(batch.num_rows(), 0);
        Ok(())
    }

    #[test]
    fn test_join_filter_ast_checks_string_view_literals() -> Result<(), Box<dyn Error>> {
        let filter = string_literal_filter(
            DataType::Utf8View,
            ScalarValue::Utf8View(Some("needle".to_string())),
        );
        assert!(is_join_filter_supported_by_cudf_ast(&filter)?);
        join_filter_to_cudf_ast(&filter)?;

        let filter = string_literal_filter(
            DataType::BinaryView,
            ScalarValue::BinaryView(Some(b"needle".to_vec())),
        );
        assert!(is_join_filter_supported_by_cudf_ast(&filter)?);
        join_filter_to_cudf_ast(&filter)?;

        let filter = string_literal_filter(
            DataType::BinaryView,
            ScalarValue::BinaryView(Some(vec![0xff])),
        );
        assert!(!is_join_filter_supported_by_cudf_ast(&filter)?);
        let Err(err) = join_filter_to_cudf_ast(&filter) else {
            panic!("invalid BinaryView literal should not lower");
        };
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
        Ok(())
    }

    #[test]
    fn test_join_filter_ast_uses_null_aware_logical_or() -> Result<(), Box<dyn Error>> {
        let left_val = Arc::new(Column::new("left_val", 0)) as Arc<dyn PhysicalExpr>;
        let less_than_right = Arc::new(BinaryExpr::new(
            Arc::clone(&left_val),
            Operator::Lt,
            Arc::new(Column::new("right_val", 1)),
        )) as Arc<dyn PhysicalExpr>;
        let left_is_null = Arc::new(IsNullExpr::new(left_val)) as Arc<dyn PhysicalExpr>;
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(less_than_right, Operator::Or, left_is_null))
                as Arc<dyn PhysicalExpr>,
            filter_indices(),
            nullable_filter_schema(),
        );
        let build = Arc::new(make_nullable_table(vec![1, 2], vec![None, Some(20)])?);
        let probe = make_table(vec![1, 2], vec![25, 15])?;

        let batch = run_filtered_inner_join_with_tables(filter, build, probe)?;
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(int32_options(&batch, 0), vec![None]);
        assert_eq!(int32_values(&batch, 1), vec![25]);
        Ok(())
    }

    #[test]
    fn test_join_filter_ast_rejects_in_list() -> Result<(), Box<dyn Error>> {
        let schema = filter_schema();
        let filter_expr = in_list(
            Arc::new(Column::new("left_val", 0)) as Arc<dyn PhysicalExpr>,
            vec![
                Arc::new(Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>,
                Arc::new(Literal::new(ScalarValue::Int32(Some(30)))) as Arc<dyn PhysicalExpr>,
            ],
            &false,
            schema.as_ref(),
        )?;
        let filter = JoinFilter::new(filter_expr, filter_indices(), schema);

        assert!(!is_join_filter_supported_by_cudf_ast(&filter)?);
        Ok(())
    }

    #[test]
    fn test_parquet_filter_ast_tracks_file_columns() -> Result<(), Box<dyn Error>> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]);
        let filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = parquet_filter_to_cudf_filter(filter.as_ref(), &schema)?;
        assert_eq!(filter.column_names, vec!["id"]);

        let mismatched = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("other", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        assert!(parquet_filter_to_cudf_filter(mismatched.as_ref(), &schema).is_err());
        Ok(())
    }

    fn make_table(keys: Vec<i32>, values: Vec<i32>) -> Result<CuDFTable, Box<dyn Error>> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("val", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int32Array::from(values)),
            ],
        )?;
        Ok(execute_cudf(CuDFTable::from_arrow_host(batch))?)
    }

    fn make_nullable_table(
        keys: Vec<i32>,
        values: Vec<Option<i32>>,
    ) -> Result<CuDFTable, Box<dyn Error>> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("val", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int32Array::from(values)),
            ],
        )?;
        Ok(execute_cudf(CuDFTable::from_arrow_host(batch))?)
    }

    fn filter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("left_val", DataType::Int32, false),
            Field::new("right_val", DataType::Int32, false),
        ]))
    }

    fn nullable_filter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("left_val", DataType::Int32, true),
            Field::new("right_val", DataType::Int32, false),
        ]))
    }

    fn string_literal_filter(data_type: DataType, literal: ScalarValue) -> JoinFilter {
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left_text", 0)),
                Operator::Eq,
                Arc::new(Literal::new(literal)),
            )) as Arc<dyn PhysicalExpr>,
            vec![ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            }],
            Arc::new(Schema::new(vec![Field::new("left_text", data_type, true)])),
        )
    }

    fn filter_indices() -> Vec<ColumnIndex> {
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ]
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<i32> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    fn int32_options(batch: &RecordBatch, column: usize) -> Vec<Option<i32>> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i))
                }
            })
            .collect()
    }

    fn run_filtered_inner_join(filter: JoinFilter) -> Result<RecordBatch, Box<dyn Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30])?);
        let probe = make_table(vec![2, 3], vec![25, 25])?;
        run_filtered_inner_join_with_tables(filter, build, probe)
    }

    fn run_filtered_inner_join_with_tables(
        filter: JoinFilter,
        build: Arc<CuDFTable>,
        probe: CuDFTable,
    ) -> Result<RecordBatch, Box<dyn Error>> {
        let build_view = Arc::clone(&build).view();
        let join = execute_cudf(
            CuDFHashJoin::build(&build_view, &[0]).null_equality(CuDFNullEquality::Unequal),
        )?;
        let probe_view = probe.into_view();
        let predicate = join_filter_to_cudf_ast(&filter)?;
        let result = execute_cudf(
            join.inner(&probe_view, &[0])
                .filter(&predicate)
                .condition_tables(&build_view, &probe_view)
                .payloads(&build_view, &probe_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;
        Ok(execute_cudf(result.into_view().to_arrow_host())?)
    }
}
