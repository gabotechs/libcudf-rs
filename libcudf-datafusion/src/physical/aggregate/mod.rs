use crate::expr::expr_to_cudf_expr;
use crate::expr::CuDFColumnExpr;
use crate::physical::aggregate::op::count::CuDFCount;
use crate::planner::CuDFConfig;
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion_physical_plan::aggregates::{
    aggregate_expressions, AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::expressions::{Column, Literal};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, InputOrderMode, PhysicalExpr, PlanProperties,
};
use std::any::{type_name, Any};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

mod op;
mod stream;

pub use op::avg::avg;
pub use op::count::count;
pub use op::max::max;
pub use op::min::min;
pub use op::sum::sum;
pub(crate) use op::udf::CuDFAggregateUDF;
pub(crate) use op::CuDFAggregationOp;

/// A fully validated cuDF aggregate plan for one DataFusion `AggregateExec`.
#[derive(Debug, Clone)]
pub(crate) struct PreparedCuDFAggregate {
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    /// Physical cuDF aggregate requests. These produce the flat state columns
    /// consumed by `outputs`, not every physical aggregate is emitted directly.
    aggs: Vec<PreparedPhysicalAggregate>,
    /// DataFusion aggregate output columns in schema order. Outputs may point at
    /// physical state directly or derive from state produced by other aggregates.
    outputs: Vec<PreparedAggregateOutput>,
}

/// One physical cuDF aggregate request and state slice.
#[derive(Debug, Clone)]
pub(crate) struct PreparedPhysicalAggregate {
    pub(crate) op: Arc<dyn CuDFAggregationOp>,
    pub(crate) args: Vec<Arc<dyn PhysicalExpr>>,
    pub(crate) output_type: DataType,
}

/// One DataFusion aggregate output column, in schema order.
#[derive(Debug, Clone)]
pub(crate) struct PreparedAggregateOutput {
    expr: Arc<AggregateFunctionExpr>,
    kind: PreparedAggregateOutputKind,
}

#[derive(Debug, Clone)]
pub(crate) enum PreparedAggregateOutputKind {
    Direct {
        physical: usize,
    },
    Derived {
        op: Arc<dyn CuDFAggregationOp>,
        state: Vec<StateColumnRef>,
        output_type: DataType,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StateColumnRef {
    pub(crate) aggregate: usize,
    pub(crate) column: usize,
}

pub(crate) struct DerivedAggregateOutput {
    pub(crate) state: Vec<StateColumnRef>,
    pub(crate) output_type: DataType,
}

impl PreparedCuDFAggregate {
    fn aggr_expr(&self) -> Vec<Arc<AggregateFunctionExpr>> {
        self.outputs
            .iter()
            .map(|output| output.expr.clone())
            .collect()
    }
}

/// GPU-accelerated GROUP BY aggregate execution node.
///
/// Replaces DataFusion's `AggregateExec` for queries where all aggregate
/// functions have cuDF implementations.
#[derive(Debug)]
pub struct CuDFAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    prepared: PreparedCuDFAggregate,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let Some(prepared) =
            prepare_cudf_aggregate_parts(mode, group_by, aggr_expr, &input_schema)?
        else {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "Aggregate is not supported by cuDF".to_string(),
            ));
        };

        Self::try_new_prepared(input, prepared)
    }

    fn try_new_prepared(
        input: Arc<dyn ExecutionPlan>,
        prepared: PreparedCuDFAggregate,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mode = prepared.mode;
        let aggr_expr = prepared.aggr_expr();

        let group_by_fields = prepared.group_by.expr().len();

        let group_by_schema = prepared.group_by.group_schema(&input_schema)?;
        let group_by_exprs = group_by_schema.fields.iter().take(group_by_fields).cloned();

        let mut fields = Vec::with_capacity(group_by_fields + aggr_expr.len());

        fields.extend(group_by_exprs);

        // Partial mode emits intermediate state columns (e.g., AVG emits [count, sum]).
        // All other modes emit the final result column (e.g., AVG emits [avg]).
        if mode == AggregateMode::Partial {
            for expr in &aggr_expr {
                for field in expr.state_fields()? {
                    fields.push(field);
                }
            }
        } else {
            for expr in &aggr_expr {
                fields.push(expr.field());
            }
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata.clone(),
        ));

        let group_by_expr_mapping =
            ProjectionMapping::try_new(prepared.group_by.expr().iter().cloned(), &input.schema())?;

        let plan_properties = Arc::new(AggregateExec::compute_properties(
            &input,
            output_schema,
            &group_by_expr_mapping,
            &mode,
            &InputOrderMode::Linear,
            &aggr_expr,
        )?);

        Ok(Self {
            input,
            prepared,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CuDFAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CuDFAggregateExec: ")?;
        write!(f, "mode={:?}, ", self.prepared.mode)?;
        write!(f, "group_by=[")?;
        for (i, (expr, alias)) in self.prepared.group_by.expr().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}", alias, expr)?;
        }
        write!(f, "], aggr_expr=[")?;
        for (i, output) in self.prepared.outputs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", output.expr.name())?;
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
            self.prepared.mode,
            self.prepared.group_by.clone(),
            self.prepared.aggr_expr(),
        )?;

        Ok(Arc::new(new))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cudf_cfg = CuDFConfig::from_config_options(context.session_config().options())?;
        let aggregate_chunk_target_bytes = cudf_cfg.aggregate_chunk_target_bytes;
        let input = self.input.execute(partition, context)?;
        let stream = stream::CuDFAggregateStream::new(
            input,
            self.schema(),
            self.prepared.clone(),
            aggregate_chunk_target_bytes,
            &self.metrics,
            partition,
        )?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn prepare_cudf_aggregate(node: &AggregateExec) -> Result<Option<PreparedCuDFAggregate>> {
    prepare_cudf_aggregate_parts(
        *node.mode(),
        node.group_expr().clone(),
        node.aggr_expr().to_vec(),
        &node.input().schema(),
    )
}

fn prepare_cudf_aggregate_parts(
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    input_schema: &SchemaRef,
) -> Result<Option<PreparedCuDFAggregate>> {
    if group_by.expr().is_empty() {
        return Ok(None);
    }
    if !group_by.is_single() {
        return Ok(None);
    }
    for (expr, _) in group_by.expr() {
        if expr.as_any().downcast_ref::<Column>().is_none() {
            return Ok(None);
        }
    }
    for expr in &aggr_expr {
        if expr.is_distinct() || !expr.order_bys().is_empty() {
            return Ok(None);
        }
    }

    let aggregate_args = aggregate_expressions(&aggr_expr, &mode, group_by.expr().len())?;

    let mut candidates = Vec::with_capacity(aggr_expr.len());
    for (expr, args) in aggr_expr.into_iter().zip(aggregate_args) {
        let Some(candidate) = prepare_aggregate_candidate(expr, args, mode, input_schema)? else {
            return Ok(None);
        };
        candidates.push(candidate);
    }

    let plan = if can_reuse_derived_state(mode) {
        prepare_cudf_aggregate_with_reuse(mode, group_by, candidates, input_schema)?
    } else {
        prepare_cudf_aggregate_direct(mode, group_by, candidates)
    };

    Ok(Some(plan))
}

fn prepare_aggregate_candidate(
    expr: Arc<AggregateFunctionExpr>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    mode: AggregateMode,
    input_schema: &SchemaRef,
) -> Result<Option<AggregateCandidate>> {
    let Some(udf) = expr
        .fun()
        .inner()
        .as_any()
        .downcast_ref::<CuDFAggregateUDF>()
    else {
        return Ok(None);
    };
    let op = udf.gpu().clone();

    if !original_aggregate_args_supported(&expr)? {
        return Ok(None);
    }

    let output_type = expr.field().data_type().clone();
    let arg_types = args
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<Vec<DataType>>>()?;
    if !op.supports_input_types(mode, &arg_types, &output_type) {
        return Ok(None);
    }

    let mut converted = Vec::with_capacity(args.len());
    for arg in args {
        let Some(arg) = expr_to_cudf_aggregate_arg(arg)? else {
            return Ok(None);
        };
        converted.push(arg);
    }

    Ok(Some(AggregateCandidate {
        kind: AggregateCandidateKind::from_expr(&expr, mode),
        expr,
        op,
        args: converted,
        output_type,
    }))
}

fn prepare_cudf_aggregate_direct(
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    candidates: Vec<AggregateCandidate>,
) -> PreparedCuDFAggregate {
    let mut aggs = Vec::with_capacity(candidates.len());
    let mut outputs = Vec::with_capacity(candidates.len());
    let count_star_arg = group_by.expr()[0].0.clone();
    for candidate in &candidates {
        let physical = push_physical_aggregate(&mut aggs, candidate, &count_star_arg);
        outputs.push(PreparedAggregateOutput {
            expr: candidate.expr.clone(),
            kind: PreparedAggregateOutputKind::Direct { physical },
        });
    }

    PreparedCuDFAggregate {
        mode,
        group_by,
        aggs,
        outputs,
    }
}

fn prepare_cudf_aggregate_with_reuse(
    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    candidates: Vec<AggregateCandidate>,
    input_schema: &SchemaRef,
) -> Result<PreparedCuDFAggregate> {
    let mut aggs = Vec::with_capacity(candidates.len());
    let mut outputs_by_candidate = Vec::with_capacity(candidates.len());
    let mut state_registry = StateRegistry::default();
    let count_star_arg = group_by.expr()[0].0.clone();

    for candidate in &candidates {
        if candidate.op.can_derive_from_reusable_state() {
            outputs_by_candidate.push(None);
            continue;
        }

        let physical = push_physical_aggregate(&mut aggs, candidate, &count_star_arg);
        for (key, column) in candidate.reusable_state_keys(input_schema)? {
            state_registry.insert(
                key,
                StateColumnRef {
                    aggregate: physical,
                    column,
                },
            );
        }
        outputs_by_candidate.push(Some(physical));
    }

    let mut outputs = Vec::with_capacity(candidates.len());
    for (candidate, physical) in candidates.iter().zip(outputs_by_candidate) {
        let derived = {
            let mut state = AggregateStatePlanner::new(&mut aggs, &mut state_registry);
            candidate.op.try_prepare_derived_output(
                &candidate.args,
                &candidate.output_type,
                input_schema,
                &mut state,
            )?
        };
        if let Some(derived) = derived {
            outputs.push(PreparedAggregateOutput {
                expr: candidate.expr.clone(),
                kind: PreparedAggregateOutputKind::Derived {
                    op: candidate.op.clone(),
                    state: derived.state,
                    output_type: derived.output_type,
                },
            });
            continue;
        }

        let physical = match physical {
            Some(physical) => physical,
            None => push_physical_aggregate(&mut aggs, candidate, &count_star_arg),
        };
        outputs.push(PreparedAggregateOutput {
            expr: candidate.expr.clone(),
            kind: PreparedAggregateOutputKind::Direct { physical },
        });
    }

    Ok(PreparedCuDFAggregate {
        mode,
        group_by,
        aggs,
        outputs,
    })
}

fn push_physical_aggregate(
    aggs: &mut Vec<PreparedPhysicalAggregate>,
    candidate: &AggregateCandidate,
    count_star_arg: &Arc<dyn PhysicalExpr>,
) -> usize {
    let physical = aggs.len();
    let mut op = candidate.op.clone();
    let mut args = candidate.args.clone();
    if candidate.kind == AggregateCandidateKind::CountStar {
        op = Arc::new(CuDFCount::count_all());
        args = vec![count_star_arg.clone()];
    }
    aggs.push(PreparedPhysicalAggregate {
        op,
        args,
        output_type: candidate.output_type.clone(),
    });
    physical
}

fn can_reuse_derived_state(mode: AggregateMode) -> bool {
    matches!(
        mode,
        AggregateMode::Single | AggregateMode::SinglePartitioned
    )
}

#[derive(Clone)]
struct AggregateCandidate {
    kind: AggregateCandidateKind,
    expr: Arc<AggregateFunctionExpr>,
    op: Arc<dyn CuDFAggregationOp>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    output_type: DataType,
}

impl AggregateCandidate {
    fn reusable_state_keys(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<Vec<(ReusableStateKey, usize)>> {
        if self.kind == AggregateCandidateKind::CountStar {
            return Ok(vec![(ReusableStateKey::CountStar, 0)]);
        }

        self.op.reusable_state_keys(&self.args, input_schema)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AggregateCandidateKind {
    Regular,
    CountStar,
}

impl AggregateCandidateKind {
    fn from_expr(expr: &AggregateFunctionExpr, mode: AggregateMode) -> Self {
        if is_raw_input_mode(mode) && is_count_star(expr) {
            Self::CountStar
        } else {
            Self::Regular
        }
    }
}

fn is_raw_input_mode(mode: AggregateMode) -> bool {
    matches!(
        mode,
        AggregateMode::Single | AggregateMode::SinglePartitioned | AggregateMode::Partial
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ExprKey {
    name: String,
    index: usize,
    data_type: DataType,
}

impl ExprKey {
    pub(crate) fn try_from_single_arg(
        args: &[Arc<dyn PhysicalExpr>],
        input_schema: &SchemaRef,
    ) -> Result<Option<Self>> {
        let [arg] = args else {
            return Ok(None);
        };
        Self::try_new(arg, input_schema)
    }

    fn try_new(expr: &Arc<dyn PhysicalExpr>, input_schema: &SchemaRef) -> Result<Option<Self>> {
        let column = if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            column
        } else if let Some(column) = expr.as_any().downcast_ref::<CuDFColumnExpr>() {
            column.host_column()
        } else {
            return Ok(None);
        };
        Ok(Some(Self {
            name: column.name().to_string(),
            index: column.index(),
            data_type: expr.data_type(input_schema)?,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ReusableStateKey {
    Sum(ExprKey),
    Count(ExprKey),
    CountStar,
}

#[derive(Default)]
struct StateRegistry {
    physical_by_state: HashMap<ReusableStateKey, StateColumnRef>,
}

impl StateRegistry {
    fn insert(&mut self, key: ReusableStateKey, state: StateColumnRef) {
        self.physical_by_state.entry(key).or_insert(state);
    }

    fn get(&self, key: &ReusableStateKey) -> Option<StateColumnRef> {
        self.physical_by_state.get(key).copied()
    }
}

pub(crate) struct AggregateStatePlanner<'a> {
    aggs: &'a mut Vec<PreparedPhysicalAggregate>,
    registry: &'a mut StateRegistry,
}

impl<'a> AggregateStatePlanner<'a> {
    fn new(aggs: &'a mut Vec<PreparedPhysicalAggregate>, registry: &'a mut StateRegistry) -> Self {
        Self { aggs, registry }
    }

    pub(crate) fn get(&self, key: &ReusableStateKey) -> Option<StateColumnRef> {
        self.registry.get(key)
    }

    pub(crate) fn ensure(
        &mut self,
        key: ReusableStateKey,
        build: impl FnOnce() -> Result<PreparedPhysicalAggregate>,
    ) -> Result<StateColumnRef> {
        if let Some(state) = self.registry.get(&key) {
            return Ok(state);
        }

        // Derived outputs can require hidden physical state that is not one of
        // DataFusion's requested outputs, for example SUM(a) to finalize AVG(a).
        let physical = self.aggs.len();
        self.aggs.push(build()?);
        let state = StateColumnRef {
            aggregate: physical,
            column: 0,
        };
        self.registry.insert(key, state);
        Ok(state)
    }
}

fn is_count_star(expr: &AggregateFunctionExpr) -> bool {
    expr.fun().name().eq_ignore_ascii_case("count")
        && expr.expressions().iter().all(|arg| {
            arg.as_any()
                .downcast_ref::<Literal>()
                .is_some_and(|literal| !literal.value().is_null())
        })
}

fn original_aggregate_args_supported(expr: &AggregateFunctionExpr) -> Result<bool> {
    // Final aggregates see only state columns, so they may look GPU-compatible
    // even when the matching partial aggregate had unsupported raw arguments.
    // Keep the whole aggregate chain on one side of the CPU/GPU boundary.
    for arg in expr.expressions() {
        if expr_to_cudf_aggregate_arg(arg)?.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn expr_to_cudf_aggregate_arg(arg: Arc<dyn PhysicalExpr>) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // A bare literal aggregate argument, e.g. COUNT(*), must keep DataFusion's
    // normal batch-length expansion. CuDFLiteral evaluates to a scalar, which is
    // correct inside binary GPU expressions but not as a direct aggregate input.
    if arg.as_any().downcast_ref::<Literal>().is_some() {
        return Ok(Some(arg));
    }

    match expr_to_cudf_expr(arg.as_ref()) {
        Ok(expr) => Ok(Some(expr)),
        Err(DataFusionError::NotImplemented(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Try to convert an `AggregateExec` to a `CuDFAggregateExec`.
///
/// Returns `Ok(None)` if any part of the aggregate is not supported by the GPU
/// implementation so the planner can keep the original CPU aggregate.
pub(crate) fn try_as_cudf_aggregate(
    node: &AggregateExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(prepared) = prepare_cudf_aggregate(node)? else {
        return Ok(None);
    };
    Ok(Some(Arc::new(CuDFAggregateExec::try_new_prepared(
        node.input().clone(),
        prepared,
    )?)))
}

#[cfg(test)]
mod test {
    use crate::assert_snapshot;
    use crate::physical::aggregate::op::avg::avg;
    use crate::physical::aggregate::op::count::count;
    use crate::physical::aggregate::op::max::max;
    use crate::physical::aggregate::op::min::min;
    use crate::physical::aggregate::op::sum::sum;
    use crate::physical::aggregate::CuDFAggregateExec;
    use crate::physical::{CuDFLoadExec, CuDFUnloadExec};
    use crate::test_utils::sort_batches;
    use crate::CuDFConfig;
    use arrow::array::{record_batch, Array, ArrayRef, Int64Array, StringArray, StringViewArray};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::ScalarValue;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::prelude::SessionConfig;
    use datafusion_expr::AggregateUDF;
    use datafusion_physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};
    use datafusion_physical_plan::expressions::{col, Literal};
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::PhysicalExpr;
    use futures_util::TryStreamExt;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;

    async fn run_group_by_test(
        agg_fn: Arc<AggregateUDF>,
        build_args: impl FnOnce(&SchemaRef) -> datafusion::error::Result<Vec<Arc<dyn PhysicalExpr>>>,
        agg_alias: &str,
    ) -> Result<String, Box<dyn Error>> {
        let batches = collect_group_by_test(
            agg_fn,
            build_args,
            agg_alias,
            Arc::new(TaskContext::default().with_session_config(
                SessionConfig::default().with_option_extension(CuDFConfig::default()),
            )),
        )
        .await?;

        let sorted = sort_batches(&batches);
        let output = pretty_format_batches(&sorted)?.to_string();
        Ok(output)
    }

    async fn collect_group_by_test(
        agg_fn: Arc<AggregateUDF>,
        build_args: impl FnOnce(&SchemaRef) -> datafusion::error::Result<Vec<Arc<dyn PhysicalExpr>>>,
        agg_alias: &str,
        task: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        let batch = record_batch!(
            ("a", Int64, [1, 4, 3]),
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
        let load = CuDFLoadExec::try_new(Arc::new(root))?;

        let group_by = PhysicalGroupBy::new_single(vec![(col("c", &schema)?, "c".to_string())]);

        let agg = AggregateExprBuilder::new(agg_fn, build_args(&schema)?)
            .schema(schema)
            .alias(agg_alias)
            .build()?;

        let aggregate = CuDFAggregateExec::try_new(
            Arc::new(load),
            AggregateMode::Single,
            group_by,
            vec![Arc::new(agg)],
        )?;

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));

        let result = unload.execute(0, task)?;
        let batches = result.try_collect::<Vec<_>>().await?;

        Ok(batches)
    }

    fn aggregate_expr(
        agg_fn: Arc<AggregateUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &SchemaRef,
        alias: &str,
    ) -> Result<Arc<datafusion_physical_plan::udaf::AggregateFunctionExpr>, Box<dyn Error>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(agg_fn, args)
                .schema(Arc::clone(schema))
                .alias(alias)
                .build()?,
        ))
    }

    fn aggregate_for_batch(
        batch: RecordBatch,
        aggs: Vec<Arc<datafusion_physical_plan::udaf::AggregateFunctionExpr>>,
    ) -> Result<CuDFAggregateExec, Box<dyn Error>> {
        let schema = batch.schema();
        let root = TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?;
        let load = CuDFLoadExec::try_new(Arc::new(root))?;
        let group_by = PhysicalGroupBy::new_single(vec![(col("c", &schema)?, "c".to_string())]);

        Ok(CuDFAggregateExec::try_new(
            Arc::new(load),
            AggregateMode::Single,
            group_by,
            aggs,
        )?)
    }

    fn state_reuse_batch(nullable_a: bool) -> Result<RecordBatch, Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, nullable_a),
            Field::new("c", DataType::Utf8, false),
        ]));
        let a: ArrayRef = if nullable_a {
            Arc::new(Int64Array::from(vec![Some(1), None, Some(4)]))
        } else {
            Arc::new(Int64Array::from(vec![1, 2, 4]))
        };
        let c: ArrayRef = Arc::new(StringArray::from(vec!["hello", "hello", "world"]));
        Ok(RecordBatch::try_new(schema, vec![a, c])?)
    }

    #[tokio::test]
    async fn test_group_by_sum() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(sum(), |s| Ok(vec![col("a", s)?]), "SUM(a)").await?;

        // Note: cuDF's SUM always returns Int64 for integer inputs
        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | SUM(a) |
        +-------+--------+
        | hello | 15     |
        | world | 9      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_sum_with_tiny_aggregate_chunk() -> Result<(), Box<dyn Error>> {
        let batches = collect_group_by_test(
            sum(),
            |s| Ok(vec![col("a", s)?]),
            "SUM(a)",
            task_ctx_with_aggregate_chunk_target_bytes(1),
        )
        .await?;

        let mut rows = grouped_i64_rows(&batches);
        rows.sort();
        assert_eq!(
            rows,
            vec![("hello".to_string(), 15), ("world".to_string(), 9)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_min() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(min(), |s| Ok(vec![col("a", s)?]), "MIN(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | MIN(a) |
        +-------+--------+
        | hello | 1      |
        | world | 3      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_max() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(max(), |s| Ok(vec![col("a", s)?]), "MAX(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | MAX(a) |
        +-------+--------+
        | hello | 4      |
        | world | 3      |
        +-------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_count() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(count(), |s| Ok(vec![col("a", s)?]), "COUNT(a)").await?;

        assert_snapshot!(output, @r"
        +-------+----------+
        | c     | COUNT(a) |
        +-------+----------+
        | hello | 6        |
        | world | 3        |
        +-------+----------+
        ");

        Ok(())
    }

    /// COUNT with a literal argument — the exact code path hit by COUNT(*) = COUNT(lit(1)).
    #[tokio::test]
    async fn test_group_by_count_literal_arg() -> Result<(), Box<dyn Error>> {
        let lit_one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let output = run_group_by_test(count(), |_| Ok(vec![lit_one.clone()]), "COUNT(*)").await?;

        assert_snapshot!(output, @r"
        +-------+----------+
        | c     | COUNT(*) |
        +-------+----------+
        | hello | 6        |
        | world | 3        |
        +-------+----------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_avg() -> Result<(), Box<dyn Error>> {
        let output = run_group_by_test(avg(), |s| Ok(vec![col("a", s)?]), "AVG(a)").await?;

        assert_snapshot!(output, @r"
        +-------+--------+
        | c     | AVG(a) |
        +-------+--------+
        | hello | 2.5    |
        | world | 3.0    |
        +-------+--------+
        ");

        Ok(())
    }

    fn cudf_task_context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default().with_session_config(
            SessionConfig::default().with_option_extension(CuDFConfig::default()),
        ))
    }

    #[tokio::test]
    async fn test_avg_reuses_sum_and_count_star_for_non_null_input() -> Result<(), Box<dyn Error>> {
        let batch = state_reuse_batch(false)?;
        let schema = batch.schema();
        let lit_one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        let aggregate = aggregate_for_batch(
            batch,
            vec![
                aggregate_expr(sum(), vec![col("a", &schema)?], &schema, "SUM(a)")?,
                aggregate_expr(count(), vec![lit_one], &schema, "COUNT(*)")?,
                aggregate_expr(avg(), vec![col("a", &schema)?], &schema, "AVG(a)")?,
            ],
        )?;

        assert_eq!(aggregate.prepared.aggs.len(), 2);

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));
        let result = unload.execute(0, cudf_task_context())?;
        let batches = result.try_collect::<Vec<_>>().await?;
        let output = pretty_format_batches(&sort_batches(&batches))?.to_string();
        assert_snapshot!(output, @r"
        +-------+--------+----------+--------+
        | c     | SUM(a) | COUNT(*) | AVG(a) |
        +-------+--------+----------+--------+
        | hello | 3      | 2        | 1.5    |
        | world | 4      | 1        | 4.0    |
        +-------+--------+----------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_nullable_avg_reuses_sum_and_count_column() -> Result<(), Box<dyn Error>> {
        let batch = state_reuse_batch(true)?;
        let schema = batch.schema();
        let aggregate = aggregate_for_batch(
            batch,
            vec![
                aggregate_expr(sum(), vec![col("a", &schema)?], &schema, "SUM(a)")?,
                aggregate_expr(count(), vec![col("a", &schema)?], &schema, "COUNT(a)")?,
                aggregate_expr(avg(), vec![col("a", &schema)?], &schema, "AVG(a)")?,
            ],
        )?;

        assert_eq!(aggregate.prepared.aggs.len(), 2);

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));
        let result = unload.execute(0, cudf_task_context())?;
        let batches = result.try_collect::<Vec<_>>().await?;
        let output = pretty_format_batches(&sort_batches(&batches))?.to_string();
        assert_snapshot!(output, @r"
        +-------+--------+----------+--------+
        | c     | SUM(a) | COUNT(a) | AVG(a) |
        +-------+--------+----------+--------+
        | hello | 1      | 1        | 1.0    |
        | world | 4      | 1        | 4.0    |
        +-------+--------+----------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_derived_avg_preserves_output_order() -> Result<(), Box<dyn Error>> {
        let batch = state_reuse_batch(false)?;
        let schema = batch.schema();
        let lit_one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        let aggregate = aggregate_for_batch(
            batch,
            vec![
                aggregate_expr(avg(), vec![col("a", &schema)?], &schema, "AVG(a)")?,
                aggregate_expr(sum(), vec![col("a", &schema)?], &schema, "SUM(a)")?,
                aggregate_expr(count(), vec![lit_one], &schema, "COUNT(*)")?,
            ],
        )?;

        assert_eq!(aggregate.prepared.aggs.len(), 2);

        let unload = CuDFUnloadExec::new(Arc::new(aggregate));
        let result = unload.execute(0, cudf_task_context())?;
        let batches = result.try_collect::<Vec<_>>().await?;
        let output = pretty_format_batches(&sort_batches(&batches))?.to_string();
        assert_snapshot!(output, @r"
        +-------+--------+--------+----------+
        | c     | AVG(a) | SUM(a) | COUNT(*) |
        +-------+--------+--------+----------+
        | hello | 1.5    | 3      | 2        |
        | world | 4.0    | 4      | 1        |
        +-------+--------+--------+----------+
        ");

        Ok(())
    }

    #[test]
    fn test_nullable_avg_does_not_reuse_count_star() -> Result<(), Box<dyn Error>> {
        let batch = state_reuse_batch(true)?;
        let schema = batch.schema();
        let lit_one: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        let aggregate = aggregate_for_batch(
            batch,
            vec![
                aggregate_expr(sum(), vec![col("a", &schema)?], &schema, "SUM(a)")?,
                aggregate_expr(count(), vec![lit_one], &schema, "COUNT(*)")?,
                aggregate_expr(avg(), vec![col("a", &schema)?], &schema, "AVG(a)")?,
            ],
        )?;

        assert_eq!(aggregate.prepared.aggs.len(), 3);
        Ok(())
    }

    fn task_ctx_with_aggregate_chunk_target_bytes(bytes: usize) -> Arc<TaskContext> {
        let cudf_config = CuDFConfig {
            aggregate_chunk_target_bytes: bytes,
            ..CuDFConfig::default()
        };
        let session_config = SessionConfig::new().with_option_extension(cudf_config);

        Arc::new(TaskContext::new(
            None,
            "aggregate-test".to_string(),
            session_config,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::new(RuntimeEnv::default()),
        ))
    }

    fn grouped_i64_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
        assert_eq!(batches.len(), 1, "expected one aggregate output batch");
        let batch = &batches[0];
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected int64 sums");

        (0..batch.num_rows())
            .map(|i| (string_value(batch.column(0).as_ref(), i), sums.value(i)))
            .collect()
    }

    fn string_value(array: &dyn Array, row: usize) -> String {
        if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
            return array.value(row).to_string();
        }
        if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
            return array.value(row).to_string();
        }
        panic!("expected string group keys, got {}", array.data_type());
    }
}

/// Integration tests: full SQL pipeline through TestFramework against real weather data.
///
/// Tests using `check_query_results` omit ORDER BY — the helper sorts rows before
/// comparing, keeping plans free of sort operators. Float tests (AVG, mixed aggregates)
/// keep ORDER BY and use `assert_batches_approx_eq` to absorb last-ULP differences.
#[cfg(test)]
mod integration {
    use crate::assert_snapshot;
    use crate::test_utils::{check_query_results, sort_batches, TestFramework};
    use arrow::array::{Array, Float64Array, RecordBatch};
    use std::error::Error;

    /// Absorbs last-ULP differences between cuDF and DataFusion float arithmetic.
    /// Used only for tests that produce Float64 results (AVG, mixed aggregates).
    fn assert_batches_approx_eq(gpu: &[RecordBatch], cpu: &[RecordBatch], decimals: u32) {
        let factor = 10f64.powi(decimals as i32);
        assert_eq!(gpu.len(), cpu.len(), "batch count mismatch");
        for (b, (g, c)) in gpu.iter().zip(cpu.iter()).enumerate() {
            assert_eq!(g.num_rows(), c.num_rows(), "batch {b}: row count mismatch");
            assert_eq!(g.num_columns(), c.num_columns(), "batch {b}: column count");
            for col in 0..g.num_columns() {
                let gc = g.column(col);
                let cc = c.column(col);
                if let (Some(gf), Some(cf)) = (
                    gc.as_any().downcast_ref::<Float64Array>(),
                    cc.as_any().downcast_ref::<Float64Array>(),
                ) {
                    for row in 0..gf.len() {
                        let gv = (gf.value(row) * factor).round() / factor;
                        let cv = (cf.value(row) * factor).round() / factor;
                        assert_eq!(gv, cv, "batch {b}, col {col}, row {row}");
                    }
                } else {
                    assert_eq!(gc.as_ref(), cc.as_ref(), "batch {b}, col {col}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sum() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", SUM("Rainfall") as total_rain FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total_rain]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT("Rainfall") as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[count(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// AVG produces Float64 — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_avg() -> Result<(), Box<dyn Error>> {
        let sql =
            r#"SELECT "RainToday", AVG("MinTemp") as avg_min FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, avg(weather.MinTemp)@1 as avg_min]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[avg(weather.MinTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_min_max() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, min(weather.MinTemp)@1 as lo, max(weather.MaxTemp)@2 as hi]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@2], aggr_expr=[min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Contains AVG — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_multiple_aggregates() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT("Rainfall") as n, SUM("Rainfall") as total, AVG("MaxTemp") as avg_max, MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=1; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(weather.Rainfall)@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@3], aggr_expr=[count(weather.Rainfall), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_star_mixed() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n, SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 1).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total]
            CuDFAggregateExec: mode=Single, group_by=[RainToday@RainToday@1], aggr_expr=[count(Int64(1)), sum(weather.Rainfall)]
              CuDFLoadExec
                DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_sum() -> Result<(), Box<dyn Error>> {
        let sql =
            r#"SELECT "RainToday", SUM("Rainfall") as total FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, sum(weather.Rainfall)@1 as total]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[sum(weather.Rainfall)]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@1], aggr_expr=[sum(weather.Rainfall)]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_partition_count_star() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n FROM weather GROUP BY "RainToday""#;
        let result = check_query_results(sql, 4).await?;
        assert_snapshot!(result.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1))]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Contains AVG — uses assert_batches_approx_eq to handle last-ULP differences.
    #[tokio::test]
    async fn test_multi_partition_multiple_aggs() -> Result<(), Box<dyn Error>> {
        let sql = r#"SELECT "RainToday", COUNT(*) as n, SUM("Rainfall") as total, AVG("MaxTemp") as avg_max, MIN("MinTemp") as lo, MAX("MaxTemp") as hi FROM weather GROUP BY "RainToday""#;
        let tf = TestFramework::new().await;
        let gpu = tf
            .execute(&format!(
                "SET cudf.enable=true; SET datafusion.execution.target_partitions=4; {sql}"
            ))
            .await?;
        let cpu = tf
            .execute(&format!(
                "SET datafusion.execution.target_partitions=4; {sql}"
            ))
            .await?;
        assert_batches_approx_eq(&sort_batches(&gpu.batches), &sort_batches(&cpu.batches), 10);
        assert_snapshot!(gpu.plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as n, sum(weather.Rainfall)@2 as total, avg(weather.MaxTemp)@3 as avg_max, min(weather.MinTemp)@4 as lo, max(weather.MaxTemp)@5 as hi]
            CuDFAggregateExec: mode=FinalPartitioned, group_by=[RainToday@RainToday@0], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
              CuDFLoadExec
                RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=1
                  CuDFUnloadExec
                    CuDFAggregateExec: mode=Partial, group_by=[RainToday@RainToday@3], aggr_expr=[count(Int64(1)), sum(weather.Rainfall), avg(weather.MaxTemp), min(weather.MinTemp), max(weather.MaxTemp)]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet, /testdata/weather/result-000001.parquet, /testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, RainToday], file_type=parquet
        ");
        Ok(())
    }

    /// Aggregates with unsupported functions (non-CuDFAggregateUDF) must fall back to CPU.
    #[tokio::test]
    async fn test_unsupported_agg_falls_back_to_cpu() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let sql = r#"SELECT "RainToday", BOOL_OR("RainTomorrow" = 'Yes') as any_rain FROM weather GROUP BY "RainToday" ORDER BY "RainToday""#;
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            !gpu.plan.contains("CuDFAggregateExec"),
            "expected CPU fallback"
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    /// Aggregates with unsupported argument expressions must also fall back to CPU.
    #[tokio::test]
    async fn test_unsupported_agg_arg_falls_back_to_cpu() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        let sql = r#"SELECT "RainToday", COUNT(SUBSTR("RainTomorrow", 1, 1)) as n FROM weather GROUP BY "RainToday" ORDER BY "RainToday""#;
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            !gpu.plan.contains("CuDFAggregateExec"),
            "unsupported aggregate argument should keep AggregateExec:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_sum_expression_uses_cudf_args() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        tf.execute(
            r#"CREATE TABLE decimal_sales (
                k VARCHAR,
                price DECIMAL(10, 2),
                discount DECIMAL(10, 2)
            ) AS VALUES
                ('a', 10.00, 1.25),
                ('a', 20.00, 2.50),
                ('b', 30.00, 3.75)"#,
        )
        .await?;

        let sql = "SELECT k, SUM(price - discount) AS net FROM decimal_sales GROUP BY k ORDER BY k";
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            gpu.plan.contains("CuDFAggregateExec"),
            "expected decimal aggregate expression to use cuDF:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal_avg_uses_cudf() -> Result<(), Box<dyn Error>> {
        let tf = TestFramework::new().await;
        tf.execute(
            r#"CREATE TABLE decimal_sales (
                k VARCHAR,
                price DECIMAL(12, 4)
            ) AS VALUES
                ('a', 10.0100),
                ('a', 20.0200),
                ('b', 30.0300)"#,
        )
        .await?;

        let sql = "SELECT k, AVG(price) AS avg_price FROM decimal_sales GROUP BY k ORDER BY k";
        let gpu = tf.execute(&format!("SET cudf.enable=true; {sql}")).await?;
        let cpu = tf.execute(sql).await?;
        assert!(
            gpu.plan.contains("CuDFAggregateExec"),
            "expected AVG(decimal) to use cuDF:\n{}",
            gpu.plan
        );
        assert_eq!(cpu.pretty_print, gpu.pretty_print);
        Ok(())
    }
}
