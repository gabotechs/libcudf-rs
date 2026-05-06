use crate::decimal::{decimal_div, decimal_div_on, is_decimal_division};
use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, cudf_to_columnar_value, expr_to_cudf_expr};
use arrow::array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::Operator;
use delegate::delegate;
use libcudf_rs::{cudf_binary_op, cudf_binary_op_on, CuDFBinaryOp, CuDFStream};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone)]
pub struct CuDFBinaryExpr {
    inner: BinaryExpr,

    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: CuDFBinaryOp,
    stream: Option<Arc<CuDFStream>>,
}

impl Debug for CuDFBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CuDFBinaryExpr")
            .field("inner", &self.inner)
            .field("op", &self.op)
            .field("streamed", &self.stream.is_some())
            .finish()
    }
}

impl Eq for CuDFBinaryExpr {}

impl PartialEq for CuDFBinaryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for CuDFBinaryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl CuDFBinaryExpr {
    pub fn from_host(expr: BinaryExpr) -> Result<Self, DataFusionError> {
        let left = expr_to_cudf_expr(expr.left().as_ref())?;
        let right = expr_to_cudf_expr(expr.right().as_ref())?;
        let op = map_op(expr.op()).ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "Operator {:?} is not supported by cuDF",
                expr.op()
            ))
        })?;
        Ok(Self {
            inner: expr,
            left,
            right,
            op,
            stream: None,
        })
    }

    pub(crate) fn with_stream(&self, stream: Arc<CuDFStream>) -> Self {
        Self {
            inner: self.inner.clone(),
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            op: self.op,
            stream: Some(stream),
        }
    }
}

impl Display for CuDFBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        Display::fmt(&self.inner, f)
    }
}

impl PhysicalExpr for CuDFBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let expected = self.data_type(batch.schema_ref())?;

        // cuDF requires max precision for decimal output types.
        let cudf_output_type = match &expected {
            DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
            DataType::Decimal32(_, scale) => DataType::Decimal32(9, *scale),
            DataType::Decimal64(_, scale) => DataType::Decimal64(18, *scale),
            _ => expected.clone(),
        };

        let lhs_value = self.left.evaluate(batch)?;
        let rhs_value = self.right.evaluate(batch)?;
        let lhs_type = lhs_value.data_type();
        let rhs_type = rhs_value.data_type();
        let lhs = columnar_value_to_cudf(lhs_value)?;
        let rhs = columnar_value_to_cudf(rhs_value)?;

        let result = if self.op == CuDFBinaryOp::Div
            && is_decimal_division(&expected, &lhs_type, &rhs_type)
        {
            match self.stream.as_deref() {
                Some(stream) => {
                    decimal_div_on(lhs, rhs, &lhs_type, &rhs_type, &cudf_output_type, stream)?
                }
                None => decimal_div(lhs, rhs, &lhs_type, &rhs_type, &cudf_output_type)?,
            }
        } else {
            match self.stream.as_deref() {
                Some(stream) => cudf_binary_op_on(lhs, rhs, self.op, &cudf_output_type, stream),
                None => cudf_binary_op(lhs, rhs, self.op, &cudf_output_type),
            }
            .map_err(cudf_to_df)?
        };
        Ok(cudf_to_columnar_value(result))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let expr = BinaryExpr::new(
            Arc::clone(&children[0]),
            *self.inner.op(),
            Arc::clone(&children[1]),
        );
        let mut next = Self::from_host(expr)?;
        next.stream.clone_from(&self.stream);
        Ok(Arc::new(next))
    }

    delegate! {
        to self.inner {
            fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result;
            fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;
            fn data_type(&self, input_schema: &Schema) -> datafusion::common::Result<DataType>;
            fn return_field(&self, input_schema: &Schema) -> datafusion::common::Result<FieldRef>;
        }
    }
}

fn map_op(op: &Operator) -> Option<CuDFBinaryOp> {
    match op {
        // Comparison operators
        Operator::Eq => Some(CuDFBinaryOp::Equal),
        Operator::NotEq => Some(CuDFBinaryOp::NotEqual),
        Operator::Lt => Some(CuDFBinaryOp::Less),
        Operator::LtEq => Some(CuDFBinaryOp::LessEqual),
        Operator::Gt => Some(CuDFBinaryOp::Greater),
        Operator::GtEq => Some(CuDFBinaryOp::GreaterEqual),

        // Arithmetic operators
        Operator::Plus => Some(CuDFBinaryOp::Add),
        Operator::Minus => Some(CuDFBinaryOp::Sub),
        Operator::Multiply => Some(CuDFBinaryOp::Mul),
        Operator::Divide => Some(CuDFBinaryOp::Div),
        Operator::Modulo => Some(CuDFBinaryOp::Mod),

        // Logical operators (DataFusion And/Or are logical, not bitwise)
        Operator::And => Some(CuDFBinaryOp::LogicalAnd),
        Operator::Or => Some(CuDFBinaryOp::LogicalOr),

        // Null-aware comparison
        Operator::IsDistinctFrom => Some(CuDFBinaryOp::NullNotEquals),
        Operator::IsNotDistinctFrom => Some(CuDFBinaryOp::NullEquals),

        // Bitwise operators
        Operator::BitwiseAnd => Some(CuDFBinaryOp::BitwiseAnd),
        Operator::BitwiseOr => Some(CuDFBinaryOp::BitwiseOr),
        Operator::BitwiseXor => Some(CuDFBinaryOp::BitwiseXor),
        Operator::BitwiseShiftRight => Some(CuDFBinaryOp::ShiftRight),
        Operator::BitwiseShiftLeft => Some(CuDFBinaryOp::ShiftLeft),

        // Integer division
        Operator::IntegerDivide => Some(CuDFBinaryOp::FloorDiv),

        // Operators not supported by cuDF binary operations
        Operator::RegexMatch => None,
        Operator::RegexIMatch => None,
        Operator::RegexNotMatch => None,
        Operator::RegexNotIMatch => None,
        Operator::LikeMatch => None,
        Operator::ILikeMatch => None,
        Operator::NotLikeMatch => None,
        Operator::NotILikeMatch => None,
        Operator::StringConcat => None,

        // PostgreSQL-specific operators (not supported)
        Operator::AtArrow => None,
        Operator::ArrowAt => None,
        Operator::Arrow => None,
        Operator::LongArrow => None,
        Operator::HashArrow => None,
        Operator::HashLongArrow => None,
        Operator::AtAt => None,
        Operator::HashMinus => None,
        Operator::AtQuestion => None,
        Operator::Question => None,
        Operator::QuestionAnd => None,
        Operator::QuestionPipe => None,
        Operator::Colon => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::TestFramework;
    use datafusion::common::assert_contains;

    #[tokio::test]
    async fn test_binary_operations() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        tf.execute(
            r#"CREATE TABLE temps (min_temp DOUBLE, max_temp DOUBLE, rainfall DOUBLE) AS VALUES
                (6.6, 13.1, 0.2)"#,
        )
        .await?;

        let host_sql = r#"
            SELECT
                min_temp + max_temp as addition,
                max_temp - min_temp as subtraction,
                min_temp * 2 as multiplication,
                max_temp / 2 as division,
                rainfall % 10 as modulo,
                min_temp = 12.2 as equal,
                min_temp != 0.0 as not_equal,
                min_temp < 15.0 as less_than,
                max_temp > 20.0 as greater_than,
                min_temp <= 12.2 as less_equal,
                max_temp >= 24.3 as greater_equal,
                (max_temp - min_temp) * 2 as complex_expr
            FROM temps
        "#;
        let cudf_sql = format!("SET cudf.enable=true; {host_sql}");

        let result = tf.execute(&cudf_sql).await?;
        assert_contains!(result.plan, "CuDF");
        assert_snapshot!(result.pretty_print, @r"
        +----------+-------------+----------------+----------+--------+-------+-----------+-----------+--------------+------------+---------------+--------------+
        | addition | subtraction | multiplication | division | modulo | equal | not_equal | less_than | greater_than | less_equal | greater_equal | complex_expr |
        +----------+-------------+----------------+----------+--------+-------+-----------+-----------+--------------+------------+---------------+--------------+
        | 19.7     | 6.5         | 13.2           | 6.55     | 0.2    | false | true      | true      | false        | true       | false         | 13.0         |
        +----------+-------------+----------------+----------+--------+-------+-----------+-----------+--------------+------------+---------------+--------------+
        ");

        // Verify against host execution
        let host_result = tf.execute(host_sql).await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);

        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_division_fractional_result() -> Result<(), Box<dyn std::error::Error>> {
        let tf = TestFramework::new().await;

        tf.execute(
            r#"CREATE TABLE ratios (num DECIMAL(10, 2), den DECIMAL(10, 2)) AS VALUES
                (3.00, 100.00),
                (42.00, 10.00)"#,
        )
        .await?;

        let host_sql = "SELECT num / den as ratio FROM ratios ORDER BY ratio";
        let result = tf
            .execute(&format!("SET cudf.enable=true; {host_sql}"))
            .await?;

        assert_contains!(result.plan, "CuDFProjectionExec");
        assert_snapshot!(result.pretty_print, @r"
        +----------+
        | ratio    |
        +----------+
        | 0.030000 |
        | 4.200000 |
        +----------+
        ");

        let host_result = tf.execute(host_sql).await?;
        assert_eq!(host_result.pretty_print, result.pretty_print);

        Ok(())
    }
}
