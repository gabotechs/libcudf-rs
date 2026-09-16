use crate::errors::cudf_to_df;
use crate::physical::normalize_scalar_for_cudf;
use arrow::array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use datafusion_physical_plan::expressions::Literal;
use datafusion_physical_plan::PhysicalExpr;
use delegate::delegate;
use libcudf_rs::{global_execution_stream, record_batch_execution_stream, CuDFScalar};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CuDFLiteral {
    inner: Literal,
    value: ScalarValue,
}

impl PartialEq for CuDFLiteral {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for CuDFLiteral {}

impl Hash for CuDFLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl CuDFLiteral {
    pub fn try_from_datafusion(inner: Literal) -> datafusion::common::Result<Self> {
        let value = normalize_scalar_for_cudf(inner.value().clone())?;
        Ok(Self { inner, value })
    }
}

impl Display for CuDFLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CuDFLiteral {
    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let stream = record_batch_execution_stream(batch)
            .map_err(cudf_to_df)?
            .map(Ok)
            .unwrap_or_else(global_execution_stream)
            .map_err(cudf_to_df)?;
        let scalar = CuDFScalar::try_from_arrow_host_on_stream(self.value.to_scalar()?, &stream)
            .map_err(cudf_to_df)?;
        let scalar: Arc<dyn arrow::array::Array> = Arc::new(scalar);
        Ok(ColumnarValue::Array(scalar))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }

    delegate! {
        to self.inner {
            fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType>;
            fn return_field(&self, input_schema: &Schema) -> datafusion::common::Result<FieldRef>;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CuDFLiteral;
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;
    use arrow_schema::DataType;
    use datafusion::common::assert_contains;
    use datafusion::common::{DataFusionError, ScalarValue};
    use datafusion_physical_plan::expressions::Literal;

    #[test]
    fn test_string_view_literals_lower_to_cudf_string() -> Result<(), Box<dyn std::error::Error>> {
        let literal = CuDFLiteral::try_from_datafusion(Literal::new(ScalarValue::Utf8View(Some(
            "needle".to_string(),
        ))))?;
        assert_eq!(literal.value.data_type(), DataType::Utf8);

        let literal = CuDFLiteral::try_from_datafusion(Literal::new(ScalarValue::BinaryView(
            Some(b"needle".to_vec()),
        )))?;
        assert_eq!(literal.value.data_type(), DataType::Utf8);

        let err =
            CuDFLiteral::try_from_datafusion(Literal::new(ScalarValue::BinaryView(Some(vec![
                0xff,
            ]))))
            .expect_err("invalid BinaryView literal should not lower to cuDF");
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
        Ok(())
    }

    #[tokio::test]
    async fn test_string_equality_filter() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;
        tf.execute(
            "CREATE TABLE items (id INT, category VARCHAR) AS VALUES \
             (1, 'BUILDING'), (2, 'FURNITURE'), (3, 'BUILDING')",
        )
        .await?;

        let host_sql = r#"SELECT id FROM items WHERE category = 'BUILDING' ORDER BY id"#;
        let result = tf
            .execute(&format!("SET cudf.enable=true; {host_sql}"))
            .await?;
        let host_result = tf.execute(host_sql).await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);

        Ok(())
    }

    #[tokio::test]
    async fn test_literal_in_expressions() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        tf.execute(
            r#"CREATE TABLE temps (min_temp DOUBLE, max_temp DOUBLE, rainfall DOUBLE) AS VALUES
                (6.6, 13.1, 0.6),
                (-1.6, 11.5, 0.0)"#,
        )
        .await?;

        let host_sql = r#"
            SELECT
                min_temp + 10.0 as temp_plus_ten,
                max_temp * 2 as temp_doubled,
                rainfall > 0.0 as has_rain
            FROM temps
        "#;
        let cudf_sql = format!("SET cudf.enable=true; {host_sql}");

        let result = tf.execute(&cudf_sql).await?;
        assert_contains!(result.plan, "CuDF");
        assert_snapshot!(result.pretty_print, @r"
        +---------------+--------------+----------+
        | temp_plus_ten | temp_doubled | has_rain |
        +---------------+--------------+----------+
        | 16.6          | 26.2         | true     |
        | 8.4           | 23.0         | false    |
        +---------------+--------------+----------+
        ");

        let host_result = tf.execute(host_sql).await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);

        Ok(())
    }
}
