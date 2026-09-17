use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, expr_to_cudf_expr};
use arrow::array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::common::{not_impl_err, DataFusionError};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::expressions::{CastExpr, Column};
use delegate::delegate;
use libcudf_rs::{cast, CuDFColumnViewOrScalar};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Eq)]
pub(crate) struct CuDFCastExpr {
    inner: CastExpr,
    expr: Arc<dyn PhysicalExpr>,
}

impl PartialEq for CuDFCastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for CuDFCastExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl CuDFCastExpr {
    pub(crate) fn try_from_datafusion(inner: CastExpr) -> Result<Self, DataFusionError> {
        if inner.cast_options().safe {
            return not_impl_err!("Safe casts are not supported by cuDF expressions");
        }
        if !contains_column(inner.expr()) {
            return not_impl_err!("Scalar casts are not supported by cuDF expressions");
        }
        let expr = expr_to_cudf_expr(inner.expr())?;
        Ok(Self { inner, expr })
    }
}

impl Display for CuDFCastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CuDFCastExpr {
    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let value = columnar_value_to_cudf(self.expr.evaluate(batch)?, batch)?;
        let CuDFColumnViewOrScalar::ColumnView(column) = value else {
            return not_impl_err!("Scalar casts are not supported by cuDF expressions");
        };
        let result = cast(&column, self.inner.cast_type()).map_err(cudf_to_df)?;
        Ok(ColumnarValue::Array(Arc::new(result.into_view())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let expr = children.swap_remove(0);
        let inner = Arc::new(self.inner.clone())
            .with_new_children(vec![Arc::clone(&expr)])?
            .downcast_ref::<CastExpr>()
            .expect("CastExpr::with_new_children should return a CastExpr")
            .clone();
        Ok(Arc::new(Self { inner, expr }))
    }

    delegate! {
        to self.inner {
            fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result;
            fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType>;
            fn return_field(&self, input_schema: &Schema) -> datafusion::common::Result<FieldRef>;
        }
    }
}

fn contains_column(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Column>().is_some() || expr.children().into_iter().any(contains_column)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::TestFramework;
    use datafusion::common::assert_contains;

    #[tokio::test]
    async fn test_decimal_cast() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;
        let host_sql = r#"
            SELECT CAST("MinTemp" AS DECIMAL(38, 15)) AS min_temp
            FROM weather
            WHERE "MinTemp" IS NOT NULL
        "#;
        let plan = tf
            .plan(&format!("SET cudf.enable=true; {host_sql}"))
            .await?;

        assert_contains!(plan.display(), "CuDFProjectionExec");
        let cudf_results = plan.execute().await?;
        let host_results = tf.execute(host_sql).await?;
        assert_eq!(host_results.pretty_print, cudf_results.pretty_print);
        Ok(())
    }
}
