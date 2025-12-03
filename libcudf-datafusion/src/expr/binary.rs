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
        let output_type = self.inner.data_type(batch.schema_ref())?;

        let lhs = self.left.evaluate(batch)?;
        let lhs_dbg = format!("{:?}", self.left);
        dbg!(lhs_dbg);
        let lhs = columnar_value_to_cudf(lhs)?;
        let rhs = self.left.evaluate(batch)?;
        let rhs = columnar_value_to_cudf(rhs)?;

        let result = cudf_binary_op(lhs, rhs, self.op, &output_type).map_err(cudf_to_df)?;

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
