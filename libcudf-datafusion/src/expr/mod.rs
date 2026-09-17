use crate::errors::cudf_to_df;
use crate::expr::binary::CuDFBinaryExpr;
use crate::expr::cast::CuDFCastExpr;
use crate::expr::literal::CuDFLiteral;
use crate::physical::normalize_scalar_for_cudf;
use arrow::array::{Array, RecordBatch};
use datafusion::common::{exec_err, not_impl_err};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, CastExpr, Column};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::expressions::Literal;
use libcudf_rs::{
    global_execution_stream, record_batch_execution_stream, CuDFColumnView, CuDFColumnViewOrScalar,
    CuDFScalar,
};
use std::sync::Arc;

pub(crate) mod ast;
mod binary;
mod cast;
mod column;
mod literal;

pub(crate) use column::CuDFColumnExpr;

pub(crate) fn columnar_value_to_cudf(
    c: ColumnarValue,
    batch: &RecordBatch,
) -> Result<CuDFColumnViewOrScalar, DataFusionError> {
    match c {
        ColumnarValue::Array(arr) => {
            if let Some(cudf_col_view) = arr.as_any().downcast_ref::<CuDFColumnView>() {
                return Ok(cudf_col_view.clone().into());
            }
            if let Some(cudf_scalar) = arr.as_any().downcast_ref::<CuDFScalar>() {
                return Ok(cudf_scalar.clone().into());
            }
            exec_err!("ColumnarValue::Array is not CuDFColumnView or CuDFScalar")
        }
        ColumnarValue::Scalar(value) => {
            let value = normalize_scalar_for_cudf(value)?;
            let stream = record_batch_execution_stream(batch)
                .map_err(cudf_to_df)?
                .unwrap_or(global_execution_stream().map_err(cudf_to_df)?);
            let scalar = CuDFScalar::try_from_arrow_host_on_stream(value.to_scalar()?, &stream)
                .map_err(cudf_to_df)?;
            Ok(scalar.into())
        }
    }
}

pub(crate) fn cudf_to_columnar_value(view: impl Into<CuDFColumnViewOrScalar>) -> ColumnarValue {
    match view.into() {
        CuDFColumnViewOrScalar::ColumnView(value) => ColumnarValue::Array(Arc::new(value)),
        CuDFColumnViewOrScalar::Scalar(value) => ColumnarValue::Array(Arc::new(value)),
    }
}

pub(crate) fn expr_to_cudf_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let any = expr.as_ref();
    if let Some(binary_op) = any.downcast_ref::<BinaryExpr>() {
        return Ok(Arc::new(CuDFBinaryExpr::try_from_datafusion(
            binary_op.clone(),
        )?));
    };
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return Ok(Arc::new(CuDFCastExpr::try_from_datafusion(cast.clone())?));
    }
    if let Some(column_expr) = any.downcast_ref::<Column>() {
        return Ok(Arc::new(CuDFColumnExpr::from_datafusion(
            column_expr.clone(),
        )));
    };
    if let Some(literal) = any.downcast_ref::<Literal>() {
        return Ok(Arc::new(CuDFLiteral::try_from_datafusion(literal.clone())?));
    }
    if any.downcast_ref::<ScalarSubqueryExpr>().is_some() {
        return Ok(Arc::clone(expr));
    }

    not_impl_err!("Expression {expr} not supported in CuDF")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use libcudf_rs::{record_batch_with_schema, CuDFColumn, CuDFStream, CuDFStreamFlags};

    #[test]
    fn scalar_uses_the_batch_stream() -> Result<(), Box<dyn std::error::Error>> {
        let stream = CuDFStream::try_with_flags(CuDFStreamFlags::NonBlocking)?;
        let column =
            CuDFColumn::try_from_arrow_host_on_stream(&Int32Array::from(vec![1, 2]), &stream)?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch =
            record_batch_with_schema(vec![Arc::new(column.into_view()) as ArrayRef], &schema, 2)?;

        let CuDFColumnViewOrScalar::Scalar(scalar) =
            columnar_value_to_cudf(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))), &batch)?
        else {
            unreachable!()
        };
        assert!(scalar.execution_stream().ptr_eq(&stream));
        stream.synchronize()?;
        Ok(())
    }
}
