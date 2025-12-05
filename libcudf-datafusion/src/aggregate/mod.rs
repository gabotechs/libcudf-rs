use crate::errors::cudf_to_df;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::common::exec_err;
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
pub struct CuDFAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<CuDFAggregateExpr>,

    plan_properties: PlanProperties,
}

impl CuDFAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<CuDFAggregateExpr>,
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

impl DisplayAs for CuDFAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFAggregateExec: ")?;
        write!(f, "group_by=[")?;
        for (i, (expr, alias)) in self.group_by.expr().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}", alias, expr)?;
        }
        write!(f, "], aggr_expr=[")?;
        for (i, expr) in self.aggr_expr.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", expr.name)?;
        }
        write!(f, "]")
    }
}

impl ExecutionPlan for CuDFAggregateExec {
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
pub struct CuDFAggregateExpr {
    op: Arc<dyn CuDFAggregationOp>,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    name: String,
}

impl CuDFAggregateExpr {
    pub fn try_new(
        op: Arc<dyn CuDFAggregationOp>,
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

/// GPU aggregation operation trait for two-phase aggregation.
///
/// This trait defines the contract for aggregation operations that can be executed
/// on the GPU using cuDF's group-by functionality. Aggregations follow a two-phase
/// pattern to support distributed and streaming execution:
///
/// 1. **Partial Phase**: Each input batch is aggregated independently, producing
///    partial results (e.g., partial sums, counts).
///
/// 2. **Final Phase**: Partial results are concatenated and re-aggregated to produce
///    the final output. The `merge` function combines the final aggregation results
///    into a single column.
///
/// # Example
///
/// For SUM aggregation:
/// - `partial_requests`: Returns a SUM aggregation request for the input column
/// - `final_requests`: Returns a SUM aggregation request for the partial sums
/// - `merge`: Returns the single summed column (identity operation for SUM)
pub trait CuDFAggregationOp: Debug + Send + Sync {
    /// Returns the name of this aggregation operation.
    fn name(&self) -> &str;

    /// Returns the function signature defining valid input types.
    fn signature(&self) -> &Signature;

    /// Returns the output data type for the given input types.
    fn return_type(&self, args: &[DataType]) -> Result<DataType>;

    /// Returns the output field for the given input fields.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_types: Vec<_> = arg_fields.iter().map(|f| f.data_type()).cloned().collect();
        let data_type = self.return_type(&arg_types)?;

        Ok(Arc::new(Field::new(
            self.name(),
            data_type,
            self.is_nullable(),
        )))
    }

    /// Returns whether the aggregation result can be null.
    fn is_nullable(&self) -> bool {
        true
    }

    /// Creates cuDF aggregation requests for the partial phase.
    ///
    /// Takes the input columns and returns aggregation requests to compute
    /// partial results for each input batch.
    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Creates cuDF aggregation requests for the final phase.
    ///
    /// Takes the concatenated partial result columns and returns aggregation
    /// requests to compute the final aggregation.
    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>>;

    /// Merges the final aggregation result columns into a single output column.
    ///
    /// For most aggregations this is an identity operation (returning the single
    /// result column), but some aggregations may need to combine multiple columns.
    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView>;
}

#[derive(Debug)]
struct CuDFSum {
    signature: Signature,
}

impl CuDFSum {
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

impl CuDFAggregationOp for CuDFSum {
    fn name(&self) -> &str {
        type_name::<Self>()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, got {}", args.len());
        }

        // cuDF's SUM aggregation always returns signed types for integer inputs
        match &args[0] {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(DataType::Int64),
            DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
            dt @ DataType::Decimal128(_, _) | dt @ DataType::Decimal256(_, _) => Ok(dt.clone()),
            dt @ (DataType::Duration(_) | DataType::Interval(_)) => Ok(dt.clone()),
            dt => exec_err!("SUM does not support type {:?}", dt),
        }
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        self.final_requests(args)
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, got {}", args.len());
        }

        let view = CuDFColumnView::from_arrow(&args[0]).map_err(cudf_to_df)?;
        let mut request = AggregationRequest::new(&view);
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if args.len() != 1 {
            return exec_err!("SUM merge expects 1 argument, got {}", args.len());
        }
        Ok(args[0].clone())
    }
}

#[cfg(test)]
mod test {
    use crate::aggregate::{CuDFAggregateExec, CuDFAggregateExpr, CuDFSum};
    use crate::assert_snapshot;
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use arrow::array::record_batch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::TaskContext;
    use datafusion_physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_physical_plan::expressions::col;
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::ExecutionPlan;
    use futures_util::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_group_by_sum() -> Result<(), Box<dyn Error>> {
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
        let load = CuDFLoadExec::new(Arc::new(root));

        let group_by = PhysicalGroupBy::new_single(vec![(col("c", &schema)?, "c".to_string())]);

        let sum = CuDFAggregateExpr::try_new(
            Arc::new(CuDFSum::new()),
            vec![col("a", &schema)?],
            &schema,
        )?;

        let aggregate = CuDFAggregateExec::try_new(Arc::new(load), group_by, vec![sum])?;

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));

        let task = Arc::new(TaskContext::default());

        let result = unload.execute(0, task)?;
        let batches = result.try_collect::<Vec<_>>().await?;

        let output = pretty_format_batches(&batches)?.to_string();
        // Note: cuDF's SUM always returns Int64 for integer inputs
        assert_snapshot!(output, @r"
        +-------+----------------------------------------+
        | c     | libcudf_datafusion::aggregate::CuDFSum |
        +-------+----------------------------------------+
        | world | 9                                      |
        | hello | 15                                     |
        +-------+----------------------------------------+
        ");

        Ok(())
    }
}
