use crate::errors::cudf_to_df;
use arrow::array::ArrayRef;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::types::{
    logical_float64, logical_int16, logical_int32, logical_int64, logical_int8, logical_uint16,
    logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    Coercion, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_expr::type_coercion::aggregates::check_arg_count;
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
    aggr_expr: Vec<GpuAggregateExpr>,

    plan_properties: PlanProperties,
}

impl GpuAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<GpuAggregateExpr>,
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

#[derive(Debug, Clone)]
pub struct GpuAggregateExpr {
    op: Arc<dyn GpuAggregationOp>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    name: String,
}

impl GpuAggregateExpr {
    pub fn try_new(
        op: Arc<dyn GpuAggregationOp>,
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
    ) -> Result<Self> {
        let input_exprs_fields = arguments
            .iter()
            .map(|arg| arg.return_field(schema))
            .collect::<Result<Vec<_>>>()?;

        check_arg_count(
            op.name(),
            &input_exprs_fields,
            &op.signature().type_signature,
        )?;

        let return_field = op.return_field(&input_exprs_fields)?;

        Ok(Self {
            // TODO: temp
            name: op.name().to_string(),

            op,
            arguments,
            return_field,
        })
    }

    pub fn field(&self) -> FieldRef {
        self.return_field
            .as_ref()
            .clone()
            .with_name(&self.name)
            .into()
    }
}

trait GpuAggregationOp: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn signature(&self) -> &Signature;
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<_> = arg_fields.iter().map(|f| f.data_type()).cloned().collect();
        let data_type = self.return_type(&arg_types)?;

        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            self.is_nullable(),
        )))
    }
    fn is_nullable(&self) -> bool {
        true
    }
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;
    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;
    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView>;
}

#[derive(Debug)]
struct GpuSum {
    signature: Signature,
}

impl GpuSum {
    pub fn new() -> Self {
        Self {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    // Unsigned to u64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_uint64()),
                        vec![
                            TypeSignatureClass::Native(logical_uint8()),
                            TypeSignatureClass::Native(logical_uint16()),
                            TypeSignatureClass::Native(logical_uint32()),
                        ],
                        NativeType::UInt64,
                    )]),
                    // Signed to i64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![
                            TypeSignatureClass::Native(logical_int8()),
                            TypeSignatureClass::Native(logical_int16()),
                            TypeSignatureClass::Native(logical_int32()),
                        ],
                        NativeType::Int64,
                    )]),
                    // Floats to f64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Float],
                        NativeType::Float64,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Duration,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl GpuAggregationOp for GpuSum {
    fn name(&self) -> &str {
        type_name::<Self>()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        assert_eq!(args.len(), 1);
        Ok(DataType::Int64)
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        self.final_requests(args)
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        assert_eq!(args.len(), 1);

        let view = CuDFColumnView::from_arrow(&args[0]).map_err(cudf_to_df)?;
        let mut request = AggregationRequest::new(&view);
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        assert_eq!(args.len(), 1);
        Ok(args[0].clone())
    }
}

#[cfg(test)]
mod test {
    use crate::aggregate::{GpuAggregateExec, GpuAggregateExpr, GpuSum};
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
            ("a", UInt32, [1, 4, 3]),
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

        let sum =
            GpuAggregateExpr::try_new(Arc::new(GpuSum::new()), vec![col("a", &schema)?], &schema)?;

        let aggregate = GpuAggregateExec::try_new(load, group_by, vec![sum])?;
        let task = Arc::new(TaskContext::default());

        let result = aggregate.execute(0, task)?;
        let records = result.try_collect::<Vec<_>>().await?;

        eprintln!("{:?}", records);

        Ok(())
    }
}
