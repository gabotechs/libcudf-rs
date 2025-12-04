use crate::errors::cudf_to_df;
use arrow::array::ArrayRef;
use arrow_schema::{FieldRef, Schema};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PlanProperties,
};
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumnView};
use std::any::{type_name, Any};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

mod stream;

#[derive(Debug)]
pub struct GpuAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<GpuAggregateExpr>>,

    plan_properties: PlanProperties,
}

impl GpuAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<GpuAggregateExpr>>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let group_by_fields = {
            let num_exprs = group_by.expr().len();
            if !group_by.is_single() {
                num_exprs + 1
            } else {
                num_exprs
            }
        };

        let group_by_schema = group_by.group_schema(&input_schema)?;
        let group_by_exprs = group_by_schema.fields.iter().cloned().take(group_by_fields);

        let mut fields = Vec::with_capacity(group_by_fields + aggr_expr.len());

        fields.extend(group_by_exprs);
        for expr in &aggr_expr {
            fields.push(expr.field());
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata.clone(),
        ));

        let group_by_expr_mapping =
            ProjectionMapping::try_new(group_by.expr().into_iter().cloned(), &input.schema())?;

        let plan_properties = AggregateExec::compute_properties(
            &input,
            output_schema,
            &group_by_expr_mapping,
            &AggregateMode::Single,
            &InputOrderMode::Linear,
            &[], // TODO?
        )?;

        Ok(Self {
            input,
            group_by,
            aggr_expr,
            plan_properties,
        })
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
        let new = Self::try_new(
            children[0].clone(),
            self.group_by.clone(),
            self.aggr_expr.clone(),
        )?;

        Ok(Arc::new(new))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = stream::Stream::new(
            input,
            self.schema(),
            self.group_by.clone(),
            self.aggr_expr.clone(),
        );
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
pub struct GpuAggregateExpr {
    op: Arc<dyn GpuAggregationOp>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    name: String,
}

impl GpuAggregateExpr {
    pub fn field(&self) -> FieldRef {
        self.return_field
            .as_ref()
            .clone()
            .with_name(&self.name)
            .into()
    }
}

trait GpuAggregationOp: Debug + Send + Sync {
    fn partial_requests(&self, args: &[ArrayRef]) -> Result<Vec<AggregationRequest>>;
    fn final_requests(&self, args: &[ArrayRef]) -> Result<Vec<AggregationRequest>>;
}

#[derive(Debug)]
struct GpuSum;

impl GpuAggregationOp for GpuSum {
    fn partial_requests(&self, args: &[ArrayRef]) -> Result<Vec<AggregationRequest>> {
        self.final_requests(args)
    }

    fn final_requests(&self, args: &[ArrayRef]) -> Result<Vec<AggregationRequest>> {
        assert_eq!(args.len(), 1);

        let view = CuDFColumnView::from_arrow(&args[0]).map_err(cudf_to_df)?;
        let mut request = AggregationRequest::new(&view);
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }
}

#[cfg(test)]
mod test {
    use crate::aggregate::GpuAggregateExec;
    use crate::physical::CuDFLoadExec;
    use arrow::array::record_batch;
    use datafusion::execution::TaskContext;
    use datafusion_physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_physical_plan::expressions::col;
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::ExecutionPlan;
    use futures_util::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn run() -> Result<(), Box<dyn Error>> {
        let batch = record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["hello", "hello", "world"]),
            ("d", Float64, [4.0, 5.0, 5.0])
        )
        .expect("created batch");

        let schema = batch.schema();

        let root = TestMemoryExec::try_new(
            &[vec![batch.clone(), batch.clone(), batch]],
            schema.clone(),
            None,
        )?;
        let root = Arc::new(root);
        let load = CuDFLoadExec::new(root);
        let load = Arc::new(load);

        let group_by = PhysicalGroupBy::new_single(vec![(col("c", &schema)?, "c".to_string())]);

        let aggregate = GpuAggregateExec::try_new(load, group_by, vec![])?;
        let task = Arc::new(TaskContext::default());

        let result = aggregate.execute(0, task)?;
        let records = result.try_collect::<Vec<_>>().await?;

        eprintln!("{:?}", records);

        Ok(())
    }
}
