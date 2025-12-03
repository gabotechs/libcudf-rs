use arrow::array::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::expressions::Column;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct CuDFColumnExpr {
    inner: Column,
}

impl CuDFColumnExpr {
    pub fn from_host(inner: Column) -> Self {
        Self { inner }
    }
}

impl Display for CuDFColumnExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuDF")?;
        self.inner.fmt(f)
    }
}

impl PhysicalExpr for CuDFColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }
}
