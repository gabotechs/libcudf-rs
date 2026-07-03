use crate::deferred_operation::operation_impl::CuDFOperationImpl;
use crate::execution_policy;
use crate::keep_alive::CuDFKeepAlive;
use crate::stream_readiness::CuDFStreamDependency;
use crate::{
    CuDFAstExpression, CuDFColumn, CuDFError, CuDFExecutionContext, CuDFOperation, CuDFTable,
    CuDFTableView,
};
use arrow_schema::ArrowError;
use cxx::UniquePtr;
use libcudf_sys::{ffi, JoinKind, OutOfBoundsPolicy};
use std::sync::Arc;

use super::common::{table_ref, CuDFNullEquality, SelectedPayloads};
use super::indices::{null_gather_index, JoinIndexVector};
use super::masks::{
    distinct_valid_indices, false_mask, join_index_sequence, row_indices_where_mask_is_false,
    scatter_true_into_mask, unmatched_indices_from_matches,
};
use super::output::{
    concat_join_outputs, gather_filtered_hash_join_indices, gather_hash_join_indices,
    gather_join_output, FilteredHashJoinIndicesArgs, GatherJoinOutputArgs,
};

/// Deferred build step for a reusable hash join.
///
/// Created by [`CuDFHashJoin::build`]. Execution hashes the selected build key
/// columns and keeps the build key view alive for later probes.
///
/// # Errors
///
/// Execution returns an error if a build key index is out of bounds or cuDF
/// cannot build the hash join.
pub struct CreateHashJoin<'a> {
    build: &'a CuDFTableView,
    build_on: &'a [usize],
    null_equality: CuDFNullEquality,
}

impl CreateHashJoin<'_> {
    /// Set whether null join-key values compare equal.
    ///
    /// Hash joins default to [`CuDFNullEquality::Equal`].
    pub fn null_equality(mut self, null_equality: CuDFNullEquality) -> Self {
        self.null_equality = null_equality;
        self
    }
}

impl CuDFOperation for CreateHashJoin<'_> {
    type Output = CuDFHashJoin;
}

impl CuDFOperationImpl for CreateHashJoin<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        let mut launch = execution_policy::launch(ctx)?;
        let build_keys = self.build.select_columns(self.build_on)?;
        launch.wait_table(&build_keys)?;
        let inner = ffi::hash_join_create(
            build_keys.inner(),
            self.null_equality.into_sys() as i32,
            launch.stream()?,
        )?;
        let dependency = launch.into_stream_dependency()?;
        Ok(CuDFHashJoin {
            state: Arc::new(HashJoinState::new(inner, build_keys, dependency)),
            build_payload: self.build.clone(),
            build_rows: self.build.num_rows(),
        })
    }
}

/// Retains a reusable hash join with the build keys and build-stream readiness.
pub(crate) struct HashJoinState {
    inner: UniquePtr<ffi::HashJoin>,
    _build_keys: CuDFTableView,
    build_dependency: CuDFStreamDependency,
}

impl HashJoinState {
    fn new(
        inner: UniquePtr<ffi::HashJoin>,
        build_keys: CuDFTableView,
        build_dependency: CuDFStreamDependency,
    ) -> Self {
        Self {
            inner,
            _build_keys: build_keys,
            build_dependency,
        }
    }

    fn inner(&self) -> Result<&ffi::HashJoin, CuDFError> {
        self.inner
            .as_ref()
            .ok_or(CuDFError::NullHandle("hash join"))
    }

    fn wait_ready(&self, launch: &execution_policy::OperationLaunch<'_>) -> Result<(), CuDFError> {
        self.build_dependency.wait_on_stream(launch.stream()?)
    }
}

/// Reusable hash join built from a fixed build-side key table.
///
/// Build the hash join once with [`CuDFHashJoin::build`], then probe it with one
/// or more tables using [`inner`](Self::inner) or [`left`](Self::left). Cloning
/// this value shares the underlying cuDF hash join.
#[derive(Clone)]
pub struct CuDFHashJoin {
    pub(super) state: Arc<HashJoinState>,
    build_payload: CuDFTableView,
    build_rows: usize,
}

impl CuDFHashJoin {
    /// Create a deferred reusable hash join from selected build key columns.
    ///
    /// The build table also becomes the default build payload gathered by later
    /// probes. Use [`JoinProbe::payloads`] to override payload tables.
    pub fn build<'a>(build: &'a CuDFTableView, build_on: &'a [usize]) -> CreateHashJoin<'a> {
        CreateHashJoin {
            build,
            build_on,
            null_equality: CuDFNullEquality::Equal,
        }
    }

    /// Probe this hash join for inner matches.
    ///
    /// The output contains matching build payload columns followed by matching
    /// probe payload columns.
    pub fn inner<'a>(&'a self, probe: &'a CuDFTableView, probe_on: &'a [usize]) -> JoinProbe<'a> {
        JoinProbe::new(
            HashProbeTarget::Shared(self),
            HashProbeKind::Inner,
            probe,
            probe_on,
        )
    }

    /// Probe this hash join while preserving every probe row.
    ///
    /// Rows without a matching build key get null build columns.
    pub fn left<'a>(&'a self, probe: &'a CuDFTableView, probe_on: &'a [usize]) -> JoinProbe<'a> {
        JoinProbe::new(
            HashProbeTarget::Shared(self),
            HashProbeKind::Left,
            probe,
            probe_on,
        )
    }

    /// Start a stateful multi-probe join session.
    ///
    /// Streaming joins record which build rows matched across probes so callers
    /// can emit unmatched build rows at the end.
    pub fn into_streaming_join(self) -> CuDFStreamingJoin {
        CuDFStreamingJoin {
            join: self,
            matched_build_mask: None,
        }
    }
}

/// Stateful reusable join session that tracks matched build rows.
///
/// Use this for full-join style workflows where the probe side arrives in
/// multiple batches. Probe batches with [`inner`](Self::inner) or
/// [`left`](Self::left), then call [`unmatched_build_rows`](Self::unmatched_build_rows).
pub struct CuDFStreamingJoin {
    join: CuDFHashJoin,
    matched_build_mask: Option<Arc<CuDFColumn>>,
}

impl CuDFStreamingJoin {
    /// Probe for inner matches and record matched build rows.
    pub fn inner<'a>(
        &'a mut self,
        probe: &'a CuDFTableView,
        probe_on: &'a [usize],
    ) -> JoinProbe<'a> {
        JoinProbe::new(
            HashProbeTarget::Streaming(self),
            HashProbeKind::Inner,
            probe,
            probe_on,
        )
    }

    /// Probe while preserving every probe row and recording matched build rows.
    pub fn left<'a>(
        &'a mut self,
        probe: &'a CuDFTableView,
        probe_on: &'a [usize],
    ) -> JoinProbe<'a> {
        JoinProbe::new(
            HashProbeTarget::Streaming(self),
            HashProbeKind::Left,
            probe,
            probe_on,
        )
    }

    /// Emit build rows that were not matched by previous probes.
    ///
    /// The returned operation requires a probe payload table so it can produce
    /// null-filled probe-side columns with the right shape.
    pub fn unmatched_build_rows(&self) -> UnmatchedBuildRows<'_> {
        UnmatchedBuildRows {
            join: self,
            build_payload: None,
            probe_payload: None,
            build_out_cols: None,
            probe_out_cols: None,
        }
    }

    fn record_matched_build_indices(
        &mut self,
        launch: &mut execution_policy::OperationLaunch<'_>,
        build_indices: Arc<JoinIndexVector>,
    ) -> Result<(), CuDFError> {
        if self.join.build_rows == 0 || build_indices.is_empty() {
            return Ok(());
        }

        let distinct_indices = distinct_valid_indices(launch, Arc::clone(&build_indices).view())?;
        if distinct_indices.is_empty() {
            return Ok(());
        }

        let mask = match &self.matched_build_mask {
            Some(mask) => Arc::clone(mask),
            None => false_mask(launch, self.join.build_rows)?,
        };
        self.matched_build_mask = Some(scatter_true_into_mask(launch, mask, distinct_indices)?);
        Ok(())
    }

    fn unmatched_build_indices(
        &self,
        launch: &mut execution_policy::OperationLaunch<'_>,
    ) -> Result<Arc<CuDFColumn>, CuDFError> {
        match &self.matched_build_mask {
            Some(mask) => {
                row_indices_where_mask_is_false(launch, self.join.build_rows, Arc::clone(mask))
            }
            None => join_index_sequence(launch, self.join.build_rows, 0, 1),
        }
    }
}

#[derive(Clone, Copy)]
enum HashProbeKind {
    Inner,
    Left,
}

impl HashProbeKind {
    fn unfiltered_indices(
        self,
        join: &ffi::HashJoin,
        probe_keys: &CuDFTableView,
        launch: &execution_policy::OperationLaunch<'_>,
    ) -> Result<cxx::UniquePtr<ffi::HashJoinIndices>, CuDFError> {
        match self {
            Self::Inner => Ok(ffi::hash_join_inner_join_indices(
                join,
                probe_keys.inner(),
                launch.stream()?,
                launch.resource(),
            )?),
            Self::Left => Ok(ffi::hash_join_left_join_indices(
                join,
                probe_keys.inner(),
                launch.stream()?,
                launch.resource(),
            )?),
        }
    }
}

enum HashProbeTarget<'a> {
    Shared(&'a CuDFHashJoin),
    Streaming(&'a mut CuDFStreamingJoin),
}

impl HashProbeTarget<'_> {
    fn join(&self) -> &CuDFHashJoin {
        match self {
            Self::Shared(join) => join,
            Self::Streaming(streaming) => &streaming.join,
        }
    }
}

/// Deferred reusable hash-join probe.
///
/// Created by [`CuDFHashJoin::inner`], [`CuDFHashJoin::left`],
/// [`CuDFStreamingJoin::inner`], or [`CuDFStreamingJoin::left`]. Execution waits
/// for the build hash join, probe keys, and payload tables before gathering the
/// output.
///
/// # Errors
///
/// Execution returns an error if a selected column index is out of bounds, key
/// tables are incompatible, `condition_tables` is used without `filter`, or
/// cuDF cannot allocate the output table.
pub struct JoinProbe<'a> {
    target: HashProbeTarget<'a>,
    kind: HashProbeKind,
    probe: &'a CuDFTableView,
    probe_on: &'a [usize],
    filter: FilterConfig<'a>,
    build_payload: Option<&'a CuDFTableView>,
    probe_payload: Option<&'a CuDFTableView>,
    build_out_cols: Option<&'a [usize]>,
    probe_out_cols: Option<&'a [usize]>,
}

#[derive(Clone, Copy)]
enum FilterConfig<'a> {
    None,
    Predicate {
        predicate: &'a CuDFAstExpression,
        build_conditional: Option<&'a CuDFTableView>,
        probe_conditional: Option<&'a CuDFTableView>,
    },
    ConditionTablesWithoutPredicate {
        build_conditional: &'a CuDFTableView,
        probe_conditional: &'a CuDFTableView,
    },
}

impl<'a> JoinProbe<'a> {
    fn new(
        target: HashProbeTarget<'a>,
        kind: HashProbeKind,
        probe: &'a CuDFTableView,
        probe_on: &'a [usize],
    ) -> Self {
        Self {
            target,
            kind,
            probe,
            probe_on,
            filter: FilterConfig::None,
            build_payload: None,
            probe_payload: None,
            build_out_cols: None,
            probe_out_cols: None,
        }
    }

    /// Filter equality matches with an AST predicate.
    ///
    /// The predicate is evaluated after equality matching. By default it reads
    /// from the build and probe payload tables; use [`condition_tables`](Self::condition_tables)
    /// to provide different tables for predicate column references.
    pub fn filter(mut self, predicate: &'a CuDFAstExpression) -> Self {
        let (build_conditional, probe_conditional) = match self.filter {
            FilterConfig::Predicate {
                build_conditional,
                probe_conditional,
                ..
            } => (build_conditional, probe_conditional),
            FilterConfig::ConditionTablesWithoutPredicate {
                build_conditional,
                probe_conditional,
            } => (Some(build_conditional), Some(probe_conditional)),
            _ => (None, None),
        };
        self.filter = FilterConfig::Predicate {
            predicate,
            build_conditional,
            probe_conditional,
        };
        self
    }

    /// Set tables referenced by AST left/right column references.
    ///
    /// This is only valid with [`filter`](Self::filter). Calling
    /// `condition_tables` without a predicate returns an execution error.
    pub fn condition_tables(
        mut self,
        build_conditional: &'a CuDFTableView,
        probe_conditional: &'a CuDFTableView,
    ) -> Self {
        self.filter = match self.filter {
            FilterConfig::Predicate { predicate, .. } => FilterConfig::Predicate {
                predicate,
                build_conditional: Some(build_conditional),
                probe_conditional: Some(probe_conditional),
            },
            FilterConfig::None | FilterConfig::ConditionTablesWithoutPredicate { .. } => {
                FilterConfig::ConditionTablesWithoutPredicate {
                    build_conditional,
                    probe_conditional,
                }
            }
        };
        self
    }

    /// Override the build/probe payload tables gathered into the output.
    ///
    /// Payload tables determine output columns. They also supply predicate
    /// columns unless [`condition_tables`](Self::condition_tables) is set.
    pub fn payloads(
        mut self,
        build_payload: &'a CuDFTableView,
        probe_payload: &'a CuDFTableView,
    ) -> Self {
        self.build_payload = Some(build_payload);
        self.probe_payload = Some(probe_payload);
        self
    }

    /// Gather only selected build payload columns.
    ///
    /// Column indices refer to the build payload table.
    pub fn select_build(mut self, cols: &'a [usize]) -> Self {
        self.build_out_cols = Some(cols);
        self
    }

    /// Gather only selected probe payload columns.
    ///
    /// Column indices refer to the probe payload table.
    pub fn select_probe(mut self, cols: &'a [usize]) -> Self {
        self.probe_out_cols = Some(cols);
        self
    }
}

impl CuDFOperation for JoinProbe<'_> {
    type Output = CuDFTable;
}

impl CuDFOperationImpl for JoinProbe<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        execute_probe(ctx, self)
    }
}

/// Deferred emission of unmatched build rows from a streaming join.
///
/// The output contains unmatched build payload rows and null-filled probe
/// payload columns. A probe payload table is required to define the probe-side
/// output schema.
///
/// # Errors
///
/// Execution returns an error if no probe payload table is provided, a selected
/// output column is out of bounds, or cuDF cannot allocate the output table.
pub struct UnmatchedBuildRows<'a> {
    join: &'a CuDFStreamingJoin,
    build_payload: Option<&'a CuDFTableView>,
    probe_payload: Option<&'a CuDFTableView>,
    build_out_cols: Option<&'a [usize]>,
    probe_out_cols: Option<&'a [usize]>,
}

impl<'a> UnmatchedBuildRows<'a> {
    /// Set the probe payload table used to define null probe-side columns.
    pub fn probe_payload(mut self, probe_payload: &'a CuDFTableView) -> Self {
        self.probe_payload = Some(probe_payload);
        self
    }

    /// Override build/probe payload tables gathered into the output.
    pub fn payloads(
        mut self,
        build_payload: &'a CuDFTableView,
        probe_payload: &'a CuDFTableView,
    ) -> Self {
        self.build_payload = Some(build_payload);
        self.probe_payload = Some(probe_payload);
        self
    }

    /// Gather only selected build payload columns.
    ///
    /// Column indices refer to the build payload table.
    pub fn select_build(mut self, cols: &'a [usize]) -> Self {
        self.build_out_cols = Some(cols);
        self
    }

    /// Gather only selected probe payload columns.
    ///
    /// Column indices refer to the probe payload table.
    pub fn select_probe(mut self, cols: &'a [usize]) -> Self {
        self.probe_out_cols = Some(cols);
        self
    }
}

impl CuDFOperation for UnmatchedBuildRows<'_> {
    type Output = CuDFTable;
}

impl CuDFOperationImpl for UnmatchedBuildRows<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        let mut launch = execution_policy::launch(ctx)?;
        self.join.join.state.wait_ready(&launch)?;
        launch.keep_alive(CuDFKeepAlive::HashJoinState {
            _state: Arc::clone(&self.join.join.state),
        });

        let probe_payload = self.probe_payload.ok_or_else(|| {
            CuDFError::ArrowError(ArrowError::InvalidArgumentError(
                "unmatched_build_rows requires a probe payload table".to_string(),
            ))
        })?;
        let build_payload = self.build_payload.unwrap_or(&self.join.join.build_payload);
        let payloads = SelectedPayloads::new(
            build_payload,
            probe_payload,
            self.build_out_cols,
            self.probe_out_cols,
        )?;
        launch.wait_table(payloads.left_view())?;
        launch.wait_table(payloads.right_view())?;

        let unmatched_build_indices = self.join.unmatched_build_indices(&mut launch)?;
        let null_probe_indices = join_index_sequence(
            &mut launch,
            unmatched_build_indices.len(),
            null_gather_index(),
            0,
        )?;
        let unmatched_build_indices_view = Arc::clone(&unmatched_build_indices).view();
        let null_probe_indices_view = Arc::clone(&null_probe_indices).view();
        launch.wait_column(&unmatched_build_indices_view)?;
        launch.wait_column(&null_probe_indices_view)?;

        let result = gather_join_output(
            &mut launch,
            GatherJoinOutputArgs {
                left_payload: payloads.left()?,
                right_payload: payloads.right()?,
                left_indices: unmatched_build_indices_view.inner(),
                right_indices: null_probe_indices_view.inner(),
                left_policy: OutOfBoundsPolicy::DontCheck,
                right_policy: OutOfBoundsPolicy::Nullify,
            },
        )?;
        launch.ready_table(result)
    }
}

fn execute_probe(ctx: &CuDFExecutionContext, probe: JoinProbe<'_>) -> Result<CuDFTable, CuDFError> {
    let JoinProbe {
        target,
        kind,
        probe,
        probe_on,
        filter,
        build_payload,
        probe_payload,
        build_out_cols,
        probe_out_cols,
    } = probe;

    let join = target.join().clone();
    let mut launch = execution_policy::launch(ctx)?;
    join.state.wait_ready(&launch)?;
    launch.keep_alive(CuDFKeepAlive::HashJoinState {
        _state: Arc::clone(&join.state),
    });

    let probe_keys = probe.select_columns(probe_on)?;
    let build_payload = build_payload.unwrap_or(&join.build_payload);
    let probe_payload = probe_payload.unwrap_or(probe);
    let payloads =
        SelectedPayloads::new(build_payload, probe_payload, build_out_cols, probe_out_cols)?;
    launch.wait_table(&probe_keys)?;
    launch.wait_table(payloads.left_view())?;
    launch.wait_table(payloads.right_view())?;

    let result = match filter {
        FilterConfig::None => {
            let indices = kind.unfiltered_indices(join.state.inner()?, &probe_keys, &launch)?;
            let (matched, build_indices) = gather_hash_join_indices(
                &mut launch,
                indices,
                payloads.left()?,
                payloads.right()?,
                match kind {
                    HashProbeKind::Inner => OutOfBoundsPolicy::DontCheck,
                    HashProbeKind::Left => OutOfBoundsPolicy::Nullify,
                },
                OutOfBoundsPolicy::DontCheck,
            )?;
            record_streaming_matches(&mut launch, target, build_indices)?;
            matched
        }
        FilterConfig::Predicate {
            predicate,
            build_conditional,
            probe_conditional,
        } => {
            let build_conditional = build_conditional.unwrap_or(build_payload);
            let probe_conditional = probe_conditional.unwrap_or(probe_payload);
            launch.wait_table(build_conditional)?;
            launch.wait_table(probe_conditional)?;
            let indices = HashProbeKind::Inner.unfiltered_indices(
                join.state.inner()?,
                &probe_keys,
                &launch,
            )?;
            let (matched, build_indices, probe_indices) = gather_filtered_hash_join_indices(
                &mut launch,
                indices,
                FilteredHashJoinIndicesArgs {
                    build_conditional: table_ref(build_conditional)?,
                    probe_conditional: table_ref(probe_conditional)?,
                    predicate,
                    join_kind: JoinKind::Inner,
                    build_payload: payloads.left()?,
                    probe_payload: payloads.right()?,
                    build_policy: OutOfBoundsPolicy::DontCheck,
                    probe_policy: OutOfBoundsPolicy::DontCheck,
                },
            )?;
            record_streaming_matches(&mut launch, target, build_indices)?;

            match kind {
                HashProbeKind::Inner => matched,
                HashProbeKind::Left => append_unmatched_probe_rows(
                    &mut launch,
                    matched,
                    probe.num_rows(),
                    probe_indices,
                    &payloads,
                )?,
            }
        }
        FilterConfig::ConditionTablesWithoutPredicate { .. } => {
            return Err(CuDFError::ArrowError(ArrowError::InvalidArgumentError(
                "condition_tables requires filter to be set".to_string(),
            )));
        }
    };

    launch.ready_table(result)
}

fn record_streaming_matches(
    launch: &mut execution_policy::OperationLaunch<'_>,
    target: HashProbeTarget<'_>,
    build_indices: Arc<JoinIndexVector>,
) -> Result<(), CuDFError> {
    if let HashProbeTarget::Streaming(streaming) = target {
        streaming.record_matched_build_indices(launch, build_indices)?;
    }
    Ok(())
}

fn append_unmatched_probe_rows(
    launch: &mut execution_policy::OperationLaunch<'_>,
    matched: CuDFTable,
    probe_rows: usize,
    probe_indices: Arc<JoinIndexVector>,
    payloads: &SelectedPayloads<'_>,
) -> Result<CuDFTable, CuDFError> {
    let unmatched_probe_indices =
        unmatched_indices_from_matches(launch, probe_rows, probe_indices)?;
    let null_build_indices = join_index_sequence(
        launch,
        unmatched_probe_indices.len(),
        null_gather_index(),
        0,
    )?;
    let unmatched_probe_indices_view = Arc::clone(&unmatched_probe_indices).view();
    let null_build_indices_view = Arc::clone(&null_build_indices).view();
    launch.wait_column(&unmatched_probe_indices_view)?;
    launch.wait_column(&null_build_indices_view)?;
    let probe_only = gather_join_output(
        launch,
        GatherJoinOutputArgs {
            left_payload: payloads.left()?,
            right_payload: payloads.right()?,
            left_indices: null_build_indices_view.inner(),
            right_indices: unmatched_probe_indices_view.inner(),
            left_policy: OutOfBoundsPolicy::Nullify,
            right_policy: OutOfBoundsPolicy::DontCheck,
        },
    )?;
    concat_join_outputs(launch, matched, probe_only)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CuDFAstOperator, CuDFAstTableReference};
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_table(keys: Vec<i32>, vals: Vec<i32>) -> Result<CuDFTable, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int32Array::from(vals)),
            ],
        )?;
        Ok(crate::execute_cudf(CuDFTable::from_arrow_host(batch))?)
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<i32> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    fn int32_options(batch: &RecordBatch, column: usize) -> Vec<Option<i32>> {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..values.len())
            .map(|i| values.is_valid(i).then(|| values.value(i)))
            .collect()
    }

    fn value_less_predicate() -> Result<CuDFAstExpression, CuDFError> {
        let mut predicate = CuDFAstExpression::builder();
        let build_value = predicate.column_reference(1, CuDFAstTableReference::Left)?;
        let probe_value = predicate.column_reference(1, CuDFAstTableReference::Right)?;
        predicate.binary_operation(CuDFAstOperator::Less, build_value, probe_value)?;
        Ok(predicate.finish())
    }

    #[test]
    fn test_hash_join_multiple_stateless_probes() -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30])?);
        let build_view = Arc::clone(&build).view();
        let join = crate::execute_cudf(
            CuDFHashJoin::build(&build_view, &[0]).null_equality(CuDFNullEquality::Unequal),
        )?;

        let probe_a = make_table(vec![2], vec![200])?;
        let probe_a_view = probe_a.into_view();
        let result_a = crate::execute_cudf(
            join.inner(&probe_a_view, &[0])
                .payloads(&build_view, &probe_a_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;

        let probe_b = make_table(vec![3], vec![300])?;
        let probe_b_view = probe_b.into_view();
        let result_b = crate::execute_cudf(
            join.inner(&probe_b_view, &[0])
                .payloads(&build_view, &probe_b_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;

        let batch_a = crate::execute_cudf(result_a.into_view().to_arrow_host())?;
        let batch_b = crate::execute_cudf(result_b.into_view().to_arrow_host())?;
        assert_eq!(int32_values(&batch_a, 0), vec![20]);
        assert_eq!(int32_values(&batch_a, 1), vec![200]);
        assert_eq!(int32_values(&batch_b, 0), vec![30]);
        assert_eq!(int32_values(&batch_b, 1), vec![300]);
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_probe() -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30])?);
        let build_view = Arc::clone(&build).view();
        let join = crate::execute_cudf(
            CuDFHashJoin::build(&build_view, &[0]).null_equality(CuDFNullEquality::Unequal),
        )?;
        let probe = make_table(vec![2, 3], vec![25, 25])?;
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;

        let result = crate::execute_cudf(
            join.inner(&probe_view, &[0])
                .filter(&predicate)
                .condition_tables(&build_view, &probe_view)
                .payloads(&build_view, &probe_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;

        let batch = crate::execute_cudf(result.into_view().to_arrow_host())?;
        assert_eq!(int32_values(&batch, 0), vec![20]);
        assert_eq!(int32_values(&batch, 1), vec![25]);
        Ok(())
    }

    #[test]
    fn test_streaming_join_records_unmatched_build_rows() -> Result<(), Box<dyn std::error::Error>>
    {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30])?);
        let build_view = Arc::clone(&build).view();
        let join = crate::execute_cudf(
            CuDFHashJoin::build(&build_view, &[0]).null_equality(CuDFNullEquality::Unequal),
        )?;
        let mut join = join.into_streaming_join();

        let probe = make_table(vec![2, 4], vec![200, 400])?;
        let probe_view = probe.into_view();
        let matched = crate::execute_cudf(
            join.left(&probe_view, &[0])
                .payloads(&build_view, &probe_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;
        assert_eq!(matched.num_rows(), 2);

        let batch = crate::execute_cudf(matched.into_view().to_arrow_host())?;
        assert_eq!(int32_options(&batch, 0), vec![Some(20), None]);
        assert_eq!(int32_values(&batch, 1), vec![200, 400]);

        let unmatched = crate::execute_cudf(
            join.unmatched_build_rows()
                .payloads(&build_view, &probe_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;
        let unmatched_batch = crate::execute_cudf(unmatched.into_view().to_arrow_host())?;
        let probe_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10, 30]);
        assert_eq!(probe_vals.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_filtered_streaming_left_probe_outputs_probe_only_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 2, 3], vec![10, 20, 30, 40])?);
        let build_view = Arc::clone(&build).view();
        let join = crate::execute_cudf(
            CuDFHashJoin::build(&build_view, &[0]).null_equality(CuDFNullEquality::Unequal),
        )?;
        let mut join = join.into_streaming_join();
        let probe = make_table(vec![2, 2, 4], vec![25, 35, 400])?;
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;

        let result = crate::execute_cudf(
            join.left(&probe_view, &[0])
                .filter(&predicate)
                .condition_tables(&build_view, &probe_view)
                .payloads(&build_view, &probe_view)
                .select_build(&[1])
                .select_probe(&[1]),
        )?;

        let batch = crate::execute_cudf(result.into_view().to_arrow_host())?;
        let mut pairs: Vec<_> = int32_options(&batch, 0)
            .into_iter()
            .zip(int32_options(&batch, 1))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (None, Some(400)),
                (Some(20), Some(25)),
                (Some(20), Some(35)),
                (Some(30), Some(35)),
            ]
        );
        Ok(())
    }
}
