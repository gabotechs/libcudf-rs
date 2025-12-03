use crate::errors::cudf_to_df;
use crate::expr::{columnar_value_to_cudf, cudf_to_columnar_value};
use arrow::array::RecordBatch;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::Operator;
use libcudf_rs::{cudf_binary_op, CuDFBinaryOp};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinaryOp {
    expr: BinaryExpr,
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.expr.fmt(f)
    }
}

impl PhysicalExpr for BinaryOp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let output_type = self.expr.data_type(batch.schema_ref())?;

        let lhs = self.expr.left().evaluate(batch)?;
        let lhs = columnar_value_to_cudf(lhs)?;
        let rhs = self.expr.right().evaluate(batch)?;
        let rhs = columnar_value_to_cudf(rhs)?;

        let Some(op) = map_op(self.expr.op()) else {
            return not_impl_err!("Operator {:?} is not supported by cuDF", self.expr.op());
        };

        let result = cudf_binary_op(lhs, rhs, op, &output_type).map_err(cudf_to_df)?;

        Ok(cudf_to_columnar_value(result))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.expr.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let expr = BinaryExpr::new(
            Arc::clone(&children[0]),
            *self.expr.op(),
            Arc::clone(&children[1]),
        );
        Ok(Arc::new(Self { expr }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt_sql(f)
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
