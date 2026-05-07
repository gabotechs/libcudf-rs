use crate::errors::cudf_to_df;
use crate::physical::normalize_scalar_for_cudf;
use arrow_schema::DataType;
use datafusion::common::{not_impl_err, JoinSide};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::Operator;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use libcudf_rs::{
    CuDFAstExpression, CuDFAstNode, CuDFAstOperator, CuDFAstTableReference, CuDFScalar,
};

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

    let mut ast = CuDFAstExpression::new();
    lower_expr(
        filter.expression().as_ref(),
        filter.column_indices(),
        &mut ast,
    )?;
    Ok(ast)
}

pub(crate) fn is_join_filter_supported_by_cudf_ast(
    filter: &JoinFilter,
) -> Result<bool, DataFusionError> {
    if filter.expression().data_type(filter.schema().as_ref())? != DataType::Boolean {
        return Ok(false);
    }

    Ok(can_lower_expr(
        filter.expression().as_ref(),
        filter.column_indices(),
    ))
}

fn lower_expr(
    expr: &dyn PhysicalExpr,
    column_indices: &[ColumnIndex],
    ast: &mut CuDFAstExpression,
) -> Result<CuDFAstNode, DataFusionError> {
    let any = expr.as_any();
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return lower_binary_expr(binary, column_indices, ast);
    }
    if let Some(column) = any.downcast_ref::<Column>() {
        return lower_column_expr(column, column_indices, ast);
    }
    if let Some(literal) = any.downcast_ref::<Literal>() {
        return lower_literal_expr(literal, ast);
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return lower_cast_expr(cast, column_indices, ast);
    }
    if let Some(not) = any.downcast_ref::<NotExpr>() {
        let input = lower_expr(not.arg().as_ref(), column_indices, ast)?;
        return ast
            .unary_operation(CuDFAstOperator::Not, input)
            .map_err(cudf_to_df);
    }
    if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
        let input = lower_expr(is_null.arg().as_ref(), column_indices, ast)?;
        return ast
            .unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df);
    }
    if let Some(is_not_null) = any.downcast_ref::<IsNotNullExpr>() {
        let input = lower_expr(is_not_null.arg().as_ref(), column_indices, ast)?;
        let is_null = ast
            .unary_operation(CuDFAstOperator::IsNull, input)
            .map_err(cudf_to_df)?;
        return ast
            .unary_operation(CuDFAstOperator::Not, is_null)
            .map_err(cudf_to_df);
    }

    not_impl_err!("Join filter expression {expr} is not supported by cuDF AST")
}

fn can_lower_expr(expr: &dyn PhysicalExpr, column_indices: &[ColumnIndex]) -> bool {
    let any = expr.as_any();
    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return can_lower_expr(binary.left().as_ref(), column_indices)
            && can_lower_expr(binary.right().as_ref(), column_indices)
            && (map_binary_op(binary.op()).is_some()
                || matches!(
                    binary.op(),
                    Operator::IsDistinctFrom | Operator::IsNotDistinctFrom
                ));
    }
    if let Some(column) = any.downcast_ref::<Column>() {
        return column_indices
            .get(column.index())
            .is_some_and(|column_index| column_index.side != JoinSide::None);
    }
    if any.downcast_ref::<Literal>().is_some() {
        return true;
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return matches!(
            cast.cast_type(),
            DataType::Int64 | DataType::UInt64 | DataType::Float64
        ) && can_lower_expr(cast.expr().as_ref(), column_indices);
    }
    if let Some(not) = any.downcast_ref::<NotExpr>() {
        return can_lower_expr(not.arg().as_ref(), column_indices);
    }
    if let Some(is_null) = any.downcast_ref::<IsNullExpr>() {
        return can_lower_expr(is_null.arg().as_ref(), column_indices);
    }
    if let Some(is_not_null) = any.downcast_ref::<IsNotNullExpr>() {
        return can_lower_expr(is_not_null.arg().as_ref(), column_indices);
    }

    false
}

fn lower_binary_expr(
    expr: &BinaryExpr,
    column_indices: &[ColumnIndex],
    ast: &mut CuDFAstExpression,
) -> Result<CuDFAstNode, DataFusionError> {
    let left = lower_expr(expr.left().as_ref(), column_indices, ast)?;
    let right = lower_expr(expr.right().as_ref(), column_indices, ast)?;
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
                "Join filter operator {other:?} is not supported by cuDF AST"
            ))
        })?,
    };

    ast.binary_operation(op, left, right).map_err(cudf_to_df)
}

fn lower_column_expr(
    column: &Column,
    column_indices: &[ColumnIndex],
    ast: &mut CuDFAstExpression,
) -> Result<CuDFAstNode, DataFusionError> {
    let Some(ColumnIndex { index, side }) = column_indices.get(column.index()) else {
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

fn lower_literal_expr(
    literal: &Literal,
    ast: &mut CuDFAstExpression,
) -> Result<CuDFAstNode, DataFusionError> {
    let value = normalize_scalar_for_cudf(literal.value().clone());
    let scalar = CuDFScalar::from_arrow_host(value.to_scalar()?).map_err(cudf_to_df)?;
    ast.literal(scalar).map_err(cudf_to_df)
}

fn lower_cast_expr(
    cast: &CastExpr,
    column_indices: &[ColumnIndex],
    ast: &mut CuDFAstExpression,
) -> Result<CuDFAstNode, DataFusionError> {
    let input = lower_expr(cast.expr().as_ref(), column_indices, ast)?;
    let op = match cast.cast_type() {
        DataType::Int64 => CuDFAstOperator::CastToInt64,
        DataType::UInt64 => CuDFAstOperator::CastToUint64,
        DataType::Float64 => CuDFAstOperator::CastToFloat64,
        other => {
            return not_impl_err!("Join filter cast to {other} is not supported by cuDF AST");
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
    use super::{is_join_filter_supported_by_cudf_ast, join_filter_to_cudf_ast};
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::{JoinSide, ScalarValue};
    use datafusion::physical_expr::expressions::{
        in_list, BinaryExpr, Column, IsNullExpr, Literal,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion_expr::Operator;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use libcudf_rs::{CuDFFilteredHashJoinArgs, CuDFHashJoin, CuDFNullEquality, CuDFTable};
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
        Ok(CuDFTable::from_arrow_host(batch)?)
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
        Ok(CuDFTable::from_arrow_host(batch)?)
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
        let join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;
        let probe_view = probe.into_view();
        let predicate = join_filter_to_cudf_ast(&filter)?;
        let result = join.inner_join_filtered(CuDFFilteredHashJoinArgs {
            probe: &probe_view,
            probe_on: &[0],
            build_conditional: &build_view,
            probe_conditional: &probe_view,
            predicate: &predicate,
            build_payload: &build_view,
            probe_payload: &probe_view,
            build_out_cols: Some(&[1]),
            probe_out_cols: Some(&[1]),
        })?;
        Ok(result.into_view().to_arrow_host()?)
    }
}
