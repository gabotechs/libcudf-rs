use crate::errors::cudf_to_df;
use arrow::array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_physical_plan::expressions::Literal;
use datafusion_physical_plan::PhysicalExpr;
use delegate::delegate;
use libcudf_rs::CuDFScalar;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct CuDFLiteral {
    inner: Literal,
}

impl CuDFLiteral {
    pub fn from_host(inner: Literal) -> Self {
        Self { inner }
    }
}

impl Display for CuDFLiteral {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CuDFLiteral {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let host_scalar = self.inner.value().to_scalar()?;
        Ok(ColumnarValue::Array(Arc::new(
            CuDFScalar::from_arrow_host(host_scalar).map_err(cudf_to_df)?,
        )))
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
