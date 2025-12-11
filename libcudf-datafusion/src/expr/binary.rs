use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, cudf_to_columnar_value, expr_to_cudf_expr};
use arrow::array::{AsArray, RecordBatch};
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::Operator;
use delegate::delegate;
use libcudf_rs::{cudf_binary_op, CuDFBinaryOp};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Eq)]
pub struct CuDFBinaryExpr {
    inner: BinaryExpr,

    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: CuDFBinaryOp,
}

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
        })
    }
}

impl Display for CuDFBinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CuDFBinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        // Get BOTH the expected output type (from host) AND the CuDF output type (normalized precision)
        let expected_output_type = self.data_type(batch.schema_ref())?;

        // For CuDF binary op, use normalized decimal precision (38)
        let cudf_output_type = match &expected_output_type {
            DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
            DataType::Decimal32(_, scale) => DataType::Decimal32(9, *scale),
            DataType::Decimal64(_, scale) => DataType::Decimal64(18, *scale),
            _ => expected_output_type.clone(),
        };

        let lhs = self.left.evaluate(batch)?;
        let lhs = columnar_value_to_cudf(lhs)?;
        let rhs = self.right.evaluate(batch)?;
        let rhs = columnar_value_to_cudf(rhs)?;

        let result = cudf_binary_op(lhs, rhs, self.op, &cudf_output_type).map_err(cudf_to_df)?;
        let mut result = cudf_to_columnar_value(result);

        // CuDF returns decimals with maximum precision (38), but DataFusion may expect a different precision.
        // Cast the result if needed to match the expected output type.
        if let ColumnarValue::Array(arr) = &result {
            if arr.data_type() != &expected_output_type {
                if let (DataType::Decimal128(_, result_scale), DataType::Decimal128(_expected_prec, expected_scale)) =
                    (arr.data_type(), &expected_output_type) {
                    if result_scale == expected_scale {
                        // Same scale, just different precision. Arrow's cast doesn't handle this,
                        // so we manually change the precision metadata while keeping the same i128 values.
                        let decimal_array = arr.as_primitive::<arrow::datatypes::Decimal128Type>();
                        let casted = decimal_array.clone()
                            .with_precision_and_scale(*_expected_prec, *expected_scale)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        result = ColumnarValue::Array(Arc::new(casted));
                    }
                }
            }
        }

        Ok(result)
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
        Ok(Arc::new(Self::from_host(expr)?))
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

        let host_sql = r#"
            SELECT
                "MinTemp" + "MaxTemp" as addition,
                "MaxTemp" - "MinTemp" as subtraction,
                "MinTemp" * 2 as multiplication,
                "MaxTemp" / 2 as division,
                "Rainfall" % 10 as modulo,
                "MinTemp" = 12.2 as equal,
                "MinTemp" != 0.0 as not_equal,
                "MinTemp" < 15.0 as less_than,
                "MaxTemp" > 20.0 as greater_than,
                "MinTemp" <= 12.2 as less_equal,
                "MaxTemp" >= 24.3 as greater_equal,
                ("MaxTemp" - "MinTemp") * 2 as complex_expr
            FROM weather LIMIT 1
        "#;
        let cudf_sql = format!(
            r#"
            SET datafusion.execution.target_partitions=1;
            SET cudf.enable=true;
            {host_sql}
        "#
        );

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
}
