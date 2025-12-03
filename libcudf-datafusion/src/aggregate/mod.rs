use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_expr::Partitioning;
use datafusion_physical_plan::aggregates::PhysicalGroupBy;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::{type_name, Any};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

mod stream;

#[derive(Debug)]
pub struct GpuAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,

    group_by_projection: Vec<usize>,
}

impl GpuAggregateExec {
    pub fn new(
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            input,
            plan_properties: todo!(),
            group_by_projection: vec![],
        }
    }
}

impl DisplayAs for GpuAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.fmt(f)
    }
}

impl ExecutionPlan for GpuAggregateExec {
    fn name(&self) -> &str {
        type_name::<Self>()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}

trait GpuAggregationOp {
    type State: Into<RecordBatch>;

    fn compute(&self, batch: RecordBatch) -> Result<Self::State>;

    fn merge(&self, states: Vec<Self::State>) -> Result<Self::State>;
}
