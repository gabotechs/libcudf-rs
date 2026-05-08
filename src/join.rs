use crate::data_type::arrow_type_to_cudf_data_type;
use crate::{
    CuDFAstExpression, CuDFColumn, CuDFColumnView, CuDFError, CuDFRef, CuDFScalar, CuDFTable,
    CuDFTableView,
};
use arrow::array::{BooleanArray, Int32Array, Scalar};
use arrow_schema::DataType;
use cxx::UniquePtr;
use libcudf_sys::{
    ffi, BinaryOperator, DuplicateKeepOption, JoinKind, NanEquality, NullEquality,
    OutOfBoundsPolicy, SetAsBuildTable,
};
use std::sync::Arc;

/// Arguments for probing a reusable hash join with an AST predicate.
///
/// The output columns are gathered from `build_payload` and `probe_payload`.
/// AST `Left` references read from `build_conditional`; AST `Right` references
/// read from `probe_conditional`.
#[derive(Clone, Copy)]
pub struct CuDFFilteredHashJoinArgs<'a> {
    /// Probe-side table used for equality matching.
    pub probe: &'a CuDFTableView,
    /// Probe-side equality key columns.
    pub probe_on: &'a [usize],
    /// Build-side table referenced by AST `Left` columns.
    pub build_conditional: &'a CuDFTableView,
    /// Probe-side table referenced by AST `Right` columns.
    pub probe_conditional: &'a CuDFTableView,
    /// Predicate evaluated on equality-match pairs.
    pub predicate: &'a CuDFAstExpression,
    /// Build-side table gathered into the output.
    pub build_payload: &'a CuDFTableView,
    /// Probe-side table gathered into the output.
    pub probe_payload: &'a CuDFTableView,
    /// Optional build payload columns to gather.
    pub build_out_cols: Option<&'a [usize]>,
    /// Optional probe payload columns to gather.
    pub probe_out_cols: Option<&'a [usize]>,
}

struct JoinIndexVector {
    inner: UniquePtr<ffi::DeviceIndexVector>,
}

impl JoinIndexVector {
    fn new(inner: UniquePtr<ffi::DeviceIndexVector>) -> Self {
        Self { inner }
    }

    fn len(&self) -> usize {
        self.inner.size()
    }

    fn as_sys(&self) -> &ffi::DeviceIndexVector {
        self.inner
            .as_ref()
            .expect("device index vector should not be null")
    }

    fn view(self: Arc<Self>) -> CuDFColumnView {
        let view = self.inner.view();
        CuDFColumnView::new_with_ref(view, Some(self))
    }
}

impl CuDFRef for JoinIndexVector {}

fn stream_ref(stream: &UniquePtr<ffi::CudaStreamView>) -> Result<&ffi::CudaStreamView, CuDFError> {
    stream
        .as_ref()
        .ok_or(CuDFError::NullHandle("CUDA stream view"))
}

fn resource_ref(
    resource: &UniquePtr<ffi::DeviceAsyncResourceRef>,
) -> Result<&ffi::DeviceAsyncResourceRef, CuDFError> {
    resource
        .as_ref()
        .ok_or(CuDFError::NullHandle("current device resource ref"))
}

fn null_gather_index() -> i32 {
    ffi::join_no_match()
}

fn sys_hash_join_inner_join_indices(
    join: &ffi::HashJoin,
    probe_keys: &ffi::TableView,
) -> Result<UniquePtr<ffi::HashJoinIndices>, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    Ok(ffi::hash_join_inner_join_indices(
        join,
        probe_keys,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?)
}

fn sys_hash_join_left_join_indices(
    join: &ffi::HashJoin,
    probe_keys: &ffi::TableView,
) -> Result<UniquePtr<ffi::HashJoinIndices>, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    Ok(ffi::hash_join_left_join_indices(
        join,
        probe_keys,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?)
}

fn select_cols(view: &CuDFTableView, cols: &[usize]) -> cxx::UniquePtr<ffi::TableView> {
    let indices: Vec<i32> = cols.iter().map(|&i| i as i32).collect();
    view.inner().select(&indices)
}

fn gather_join_output(
    left_payload: &ffi::TableView,
    right_payload: &ffi::TableView,
    left_indices: &ffi::ColumnView,
    right_indices: &ffi::ColumnView,
    left_policy: OutOfBoundsPolicy,
    right_policy: OutOfBoundsPolicy,
) -> Result<CuDFTable, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let left = CuDFTable::from_inner(ffi::gather_with_policy(
        left_payload,
        left_indices,
        left_policy as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    let right = CuDFTable::from_inner(ffi::gather_with_policy(
        right_payload,
        right_indices,
        right_policy as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    let mut columns = left.into_columns();
    columns.extend(right.into_columns());
    Ok(CuDFTable::from_columns(columns))
}

fn gather_join_indices(
    mut indices: UniquePtr<ffi::JoinIndices>,
    left_payload: &ffi::TableView,
    right_payload: &ffi::TableView,
    left_policy: OutOfBoundsPolicy,
    right_policy: OutOfBoundsPolicy,
) -> Result<CuDFTable, CuDFError> {
    let left_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_left()));
    let right_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_right()));
    let left_indices_view = Arc::clone(&left_indices).view();
    let right_indices_view = Arc::clone(&right_indices).view();
    gather_join_output(
        left_payload,
        right_payload,
        left_indices_view.inner(),
        right_indices_view.inner(),
        left_policy,
        right_policy,
    )
}

fn gather_hash_join_indices(
    mut indices: UniquePtr<ffi::HashJoinIndices>,
    build_payload: &ffi::TableView,
    probe_payload: &ffi::TableView,
    build_policy: OutOfBoundsPolicy,
    probe_policy: OutOfBoundsPolicy,
) -> Result<(CuDFTable, Arc<JoinIndexVector>), CuDFError> {
    let probe_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_probe()));
    let build_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_build()));
    let probe_indices_view = Arc::clone(&probe_indices).view();
    let build_indices_view = Arc::clone(&build_indices).view();
    let result = gather_join_output(
        build_payload,
        probe_payload,
        build_indices_view.inner(),
        probe_indices_view.inner(),
        build_policy,
        probe_policy,
    )?;
    Ok((result, build_indices))
}

struct FilteredHashJoinIndicesArgs<'a> {
    build_conditional: &'a ffi::TableView,
    probe_conditional: &'a ffi::TableView,
    predicate: &'a CuDFAstExpression,
    join_kind: JoinKind,
    build_payload: &'a ffi::TableView,
    probe_payload: &'a ffi::TableView,
    build_policy: OutOfBoundsPolicy,
    probe_policy: OutOfBoundsPolicy,
}

fn gather_filtered_hash_join_indices(
    mut indices: UniquePtr<ffi::HashJoinIndices>,
    args: FilteredHashJoinIndicesArgs<'_>,
) -> Result<(CuDFTable, Arc<JoinIndexVector>, Arc<JoinIndexVector>), CuDFError> {
    // Hash join returns probe/build maps. cuDF filter_join_indices expects
    // left/right maps, so pass build as left and probe as right to preserve
    // the public `[build_cols | probe_cols]` output order.
    let probe_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_probe()));
    let build_indices = Arc::new(JoinIndexVector::new(indices.pin_mut().release_build()));
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let mut filtered_indices = ffi::filter_join_indices(
        args.build_conditional,
        args.probe_conditional,
        build_indices.as_sys(),
        probe_indices.as_sys(),
        args.predicate.inner(),
        args.join_kind as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?;
    let filtered_build_indices = Arc::new(JoinIndexVector::new(
        filtered_indices.pin_mut().release_left(),
    ));
    let filtered_probe_indices = Arc::new(JoinIndexVector::new(
        filtered_indices.pin_mut().release_right(),
    ));
    let filtered_build_indices_view = Arc::clone(&filtered_build_indices).view();
    let filtered_probe_indices_view = Arc::clone(&filtered_probe_indices).view();
    let result = gather_join_output(
        args.build_payload,
        args.probe_payload,
        filtered_build_indices_view.inner(),
        filtered_probe_indices_view.inner(),
        args.build_policy,
        args.probe_policy,
    )?;
    Ok((result, filtered_build_indices, filtered_probe_indices))
}

fn concat_join_outputs(first: CuDFTable, second: CuDFTable) -> Result<CuDFTable, CuDFError> {
    if first.num_rows() == 0 {
        return Ok(second);
    }
    if second.num_rows() == 0 {
        return Ok(first);
    }
    CuDFTable::concat(vec![first.into_view(), second.into_view()])
}

fn distinct_valid_indices(indices: CuDFColumnView) -> Result<Option<Arc<CuDFColumn>>, CuDFError> {
    if indices.inner().size() == 0 {
        return Ok(None);
    }

    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let zero = int32_scalar(0)?;
    let valid_mask = Arc::new(CuDFColumn::new(ffi::binary_operation_col_scalar(
        indices.inner(),
        zero.inner(),
        BinaryOperator::GreaterEqual as i32,
        &bool_data_type(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?));
    let indices_table = CuDFTableView::from_column_views(vec![indices])?;
    let valid_indices_table = CuDFTable::from_inner(ffi::apply_boolean_mask(
        indices_table.inner(),
        Arc::clone(&valid_mask).view().inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    if valid_indices_table.num_rows() == 0 {
        return Ok(None);
    }

    let valid_indices_view = valid_indices_table.into_view();
    let distinct_indices = CuDFTable::from_inner(ffi::distinct(
        valid_indices_view.inner(),
        &[0],
        DuplicateKeepOption::KeepAny as i32,
        NullEquality::Equal as i32,
        NanEquality::AllEqual as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    if distinct_indices.num_rows() == 0 {
        return Ok(None);
    }

    Ok(Some(Arc::new(
        distinct_indices
            .into_columns()
            .into_iter()
            .next()
            .expect("distinct indices has one column"),
    )))
}

fn matched_row_mask(
    row_count: usize,
    matched_indices: Arc<JoinIndexVector>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let false_scalar = bool_scalar(false)?;
    let mask = Arc::new(CuDFColumn::new(ffi::make_column_from_scalar(
        false_scalar.inner(),
        row_count,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?));
    let Some(distinct_indices) = distinct_valid_indices(Arc::clone(&matched_indices).view())?
    else {
        return Ok(mask);
    };

    let distinct_indices_view = Arc::clone(&distinct_indices).view();
    let true_scalar = bool_scalar(true)?;
    let source = [true_scalar.inner().as_ptr()];
    let target = CuDFTableView::from_column_views(vec![Arc::clone(&mask).view()])?;
    let updated = CuDFTable::from_inner(ffi::scatter_scalars(
        &source,
        distinct_indices_view.inner(),
        target.inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    Ok(Arc::new(
        updated
            .into_columns()
            .into_iter()
            .next()
            .expect("updated matched row mask has one column"),
    ))
}

fn unmatched_indices_from_matches(
    row_count: usize,
    matched_indices: Arc<JoinIndexVector>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let all_indices = join_index_sequence(row_count, 0, 1)?;
    let matched_mask = matched_row_mask(row_count, matched_indices)?;
    let false_scalar = bool_scalar(false)?;
    let unmatched_mask = Arc::new(CuDFColumn::new(ffi::binary_operation_col_scalar(
        Arc::clone(&matched_mask).view().inner(),
        false_scalar.inner(),
        BinaryOperator::Equal as i32,
        &bool_data_type(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?));
    let all_indices_table =
        CuDFTableView::from_column_views(vec![Arc::clone(&all_indices).view()])?;
    let unmatched_table = CuDFTable::from_inner(ffi::apply_boolean_mask(
        all_indices_table.inner(),
        Arc::clone(&unmatched_mask).view().inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?);
    Ok(Arc::new(
        unmatched_table
            .into_columns()
            .into_iter()
            .next()
            .expect("unmatched indices has one column"),
    ))
}

fn join_index_sequence(size: usize, init: i32, step: i32) -> Result<Arc<CuDFColumn>, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let init_array = Int32Array::from(vec![init]);
    let step_array = Int32Array::from(vec![step]);
    let init_scalar = CuDFScalar::from_arrow_host(Scalar::new(&init_array))?;
    let step_scalar = CuDFScalar::from_arrow_host(Scalar::new(&step_array))?;
    Ok(Arc::new(CuDFColumn::new(ffi::sequence(
        size,
        init_scalar.inner(),
        step_scalar.inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?)))
}

fn int32_scalar(value: i32) -> Result<CuDFScalar, CuDFError> {
    let array = Int32Array::from(vec![value]);
    CuDFScalar::from_arrow_host(Scalar::new(&array))
}

fn bool_scalar(value: bool) -> Result<CuDFScalar, CuDFError> {
    let array = BooleanArray::from(vec![value]);
    CuDFScalar::from_arrow_host(Scalar::new(&array))
}

fn bool_data_type() -> cxx::UniquePtr<ffi::DataType> {
    arrow_type_to_cudf_data_type(&DataType::Boolean).expect("Boolean is supported by cuDF")
}

/// Reusable hash join built from a fixed build-side key table.
///
/// The build-side table is kept alive for the lifetime of this object.
pub struct CuDFHashJoin {
    inner: UniquePtr<ffi::HashJoin>,
    build_rows: usize,
    matched_build_mask: Option<Arc<CuDFColumn>>,
    _build_ref: Option<Arc<dyn CuDFRef>>,
}

/// Controls whether null join-key values compare equal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CuDFNullEquality {
    /// Null join-key values match other null join-key values.
    Equal,
    /// Null join-key values do not match anything.
    Unequal,
}

impl CuDFNullEquality {
    fn into_sys(self) -> NullEquality {
        match self {
            Self::Equal => NullEquality::Equal,
            Self::Unequal => NullEquality::Unequal,
        }
    }
}

impl CuDFHashJoin {
    /// Build a reusable hash join from the selected build-side key columns.
    pub fn try_new(
        build: &CuDFTableView,
        build_on: &[usize],
        null_equality: CuDFNullEquality,
    ) -> Result<Self, CuDFError> {
        let build_keys = select_cols(build, build_on);
        let stream = ffi::get_default_stream();
        let inner = ffi::hash_join_create(
            &build_keys,
            null_equality.into_sys() as i32,
            stream_ref(&stream)?,
        )?;
        Ok(Self {
            inner,
            build_rows: build.num_rows(),
            matched_build_mask: None,
            _build_ref: build._ref.clone(),
        })
    }

    /// Probe this hash join and gather matching payload rows.
    ///
    /// Output columns are concatenated as `[build_cols | probe_cols]`.
    pub fn inner_join(
        &self,
        probe: &CuDFTableView,
        probe_on: &[usize],
        build_payload: &CuDFTableView,
        probe_payload: &CuDFTableView,
        build_out_cols: Option<&[usize]>,
        probe_out_cols: Option<&[usize]>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(probe, probe_on);
        let selected_build_payload = build_out_cols.map(|c| select_cols(build_payload, c));
        let selected_probe_payload = probe_out_cols.map(|c| select_cols(probe_payload, c));
        let indices = sys_hash_join_inner_join_indices(&self.inner, &probe_keys)?;
        let (result, _) = gather_hash_join_indices(
            indices,
            selected_build_payload
                .as_ref()
                .unwrap_or_else(|| build_payload.inner()),
            selected_probe_payload
                .as_ref()
                .unwrap_or_else(|| probe_payload.inner()),
            OutOfBoundsPolicy::DontCheck,
            OutOfBoundsPolicy::DontCheck,
        )?;
        Ok(result)
    }

    /// Probe this hash join, filter equality matches with an AST predicate, and gather payload rows.
    ///
    /// Output columns are concatenated as `[build_cols | probe_cols]`.
    ///
    /// `build_conditional` and `probe_conditional` are the tables referenced by
    /// `predicate`; AST `Left` column references read from `build_conditional`
    /// and AST `Right` column references read from `probe_conditional`.
    ///
    /// `build_payload` and `probe_payload` are the tables gathered into the
    /// output. Use `build_out_cols` and `probe_out_cols` to gather only selected
    /// payload columns.
    ///
    /// # Errors
    ///
    /// Returns an error if probing, predicate filtering, or payload gathering
    /// fails in cuDF.
    pub fn inner_join_filtered(
        &self,
        args: CuDFFilteredHashJoinArgs<'_>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(args.probe, args.probe_on);
        let selected_build_payload = args
            .build_out_cols
            .map(|c| select_cols(args.build_payload, c));
        let selected_probe_payload = args
            .probe_out_cols
            .map(|c| select_cols(args.probe_payload, c));
        let indices = sys_hash_join_inner_join_indices(&self.inner, &probe_keys)?;
        let (result, _, _) = gather_filtered_hash_join_indices(
            indices,
            FilteredHashJoinIndicesArgs {
                build_conditional: args.build_conditional.inner(),
                probe_conditional: args.probe_conditional.inner(),
                predicate: args.predicate,
                join_kind: JoinKind::Inner,
                build_payload: selected_build_payload
                    .as_ref()
                    .unwrap_or_else(|| args.build_payload.inner()),
                probe_payload: selected_probe_payload
                    .as_ref()
                    .unwrap_or_else(|| args.probe_payload.inner()),
                build_policy: OutOfBoundsPolicy::DontCheck,
                probe_policy: OutOfBoundsPolicy::DontCheck,
            },
        )?;
        Ok(result)
    }

    /// Probe this hash join, filter equality matches with an AST predicate,
    /// record matching build rows, and emit passing inner-join rows.
    ///
    /// This is the filtered equivalent of [`CuDFHashJoin::inner_join_and_record_matches`].
    /// Only rows with a predicate-passing partner are recorded as matched.
    ///
    /// # Errors
    ///
    /// Returns an error if probing, predicate filtering, payload gathering, or
    /// match-mask updates fail in cuDF.
    pub fn inner_join_filtered_and_record_matches(
        &mut self,
        args: CuDFFilteredHashJoinArgs<'_>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(args.probe, args.probe_on);
        let selected_build_payload = args
            .build_out_cols
            .map(|c| select_cols(args.build_payload, c));
        let selected_probe_payload = args
            .probe_out_cols
            .map(|c| select_cols(args.probe_payload, c));
        let indices = sys_hash_join_inner_join_indices(&self.inner, &probe_keys)?;
        let (result, build_indices, _) = gather_filtered_hash_join_indices(
            indices,
            FilteredHashJoinIndicesArgs {
                build_conditional: args.build_conditional.inner(),
                probe_conditional: args.probe_conditional.inner(),
                predicate: args.predicate,
                join_kind: JoinKind::Inner,
                build_payload: selected_build_payload
                    .as_ref()
                    .unwrap_or_else(|| args.build_payload.inner()),
                probe_payload: selected_probe_payload
                    .as_ref()
                    .unwrap_or_else(|| args.probe_payload.inner()),
                build_policy: OutOfBoundsPolicy::DontCheck,
                probe_policy: OutOfBoundsPolicy::DontCheck,
            },
        )?;
        self.record_matched_build_indices(build_indices)?;
        Ok(result)
    }

    /// Probe this hash join, record matched build rows, and emit inner-join rows.
    pub fn inner_join_and_record_matches(
        &mut self,
        probe: &CuDFTableView,
        probe_on: &[usize],
        build_payload: &CuDFTableView,
        probe_payload: &CuDFTableView,
        build_out_cols: Option<&[usize]>,
        probe_out_cols: Option<&[usize]>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(probe, probe_on);
        let selected_build_payload = build_out_cols.map(|c| select_cols(build_payload, c));
        let selected_probe_payload = probe_out_cols.map(|c| select_cols(probe_payload, c));
        let indices = sys_hash_join_inner_join_indices(&self.inner, &probe_keys)?;
        let (result, build_indices) = gather_hash_join_indices(
            indices,
            selected_build_payload
                .as_ref()
                .unwrap_or_else(|| build_payload.inner()),
            selected_probe_payload
                .as_ref()
                .unwrap_or_else(|| probe_payload.inner()),
            OutOfBoundsPolicy::DontCheck,
            OutOfBoundsPolicy::DontCheck,
        )?;
        self.record_matched_build_indices(build_indices)?;
        Ok(result)
    }

    /// Probe this hash join preserving probe rows and record matched build rows.
    ///
    /// Output columns are concatenated as `[build_cols | probe_cols]`.
    pub fn probe_left_join_and_record_matches(
        &mut self,
        probe: &CuDFTableView,
        probe_on: &[usize],
        build_payload: &CuDFTableView,
        probe_payload: &CuDFTableView,
        build_out_cols: Option<&[usize]>,
        probe_out_cols: Option<&[usize]>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(probe, probe_on);
        let selected_build_payload = build_out_cols.map(|c| select_cols(build_payload, c));
        let selected_probe_payload = probe_out_cols.map(|c| select_cols(probe_payload, c));
        let indices = sys_hash_join_left_join_indices(&self.inner, &probe_keys)?;
        let (result, build_indices) = gather_hash_join_indices(
            indices,
            selected_build_payload
                .as_ref()
                .unwrap_or_else(|| build_payload.inner()),
            selected_probe_payload
                .as_ref()
                .unwrap_or_else(|| probe_payload.inner()),
            OutOfBoundsPolicy::Nullify,
            OutOfBoundsPolicy::DontCheck,
        )?;
        self.record_matched_build_indices(build_indices)?;
        Ok(result)
    }

    /// Probe this hash join for filtered full-join streaming.
    ///
    /// Emits predicate-passing matched rows plus probe rows that have no
    /// predicate-passing build partner in this probe batch. Call
    /// [`CuDFHashJoin::unmatched_build_rows`] after all probe batches to emit
    /// build rows with no passing partner.
    ///
    /// # Errors
    ///
    /// Returns an error if probing, predicate filtering, payload gathering, or
    /// match-mask updates fail in cuDF.
    pub fn probe_left_join_filtered_and_record_matches(
        &mut self,
        args: CuDFFilteredHashJoinArgs<'_>,
    ) -> Result<CuDFTable, CuDFError> {
        let probe_keys = select_cols(args.probe, args.probe_on);
        let selected_build_payload = args
            .build_out_cols
            .map(|c| select_cols(args.build_payload, c));
        let selected_probe_payload = args
            .probe_out_cols
            .map(|c| select_cols(args.probe_payload, c));
        let build_payload = selected_build_payload
            .as_ref()
            .unwrap_or_else(|| args.build_payload.inner());
        let probe_payload = selected_probe_payload
            .as_ref()
            .unwrap_or_else(|| args.probe_payload.inner());
        let indices = sys_hash_join_inner_join_indices(&self.inner, &probe_keys)?;
        let (matched, build_indices, probe_indices) = gather_filtered_hash_join_indices(
            indices,
            FilteredHashJoinIndicesArgs {
                build_conditional: args.build_conditional.inner(),
                probe_conditional: args.probe_conditional.inner(),
                predicate: args.predicate,
                join_kind: JoinKind::Inner,
                build_payload,
                probe_payload,
                build_policy: OutOfBoundsPolicy::DontCheck,
                probe_policy: OutOfBoundsPolicy::DontCheck,
            },
        )?;
        self.record_matched_build_indices(build_indices)?;

        let unmatched_probe_indices =
            unmatched_indices_from_matches(args.probe.num_rows(), probe_indices)?;
        let null_build_indices =
            join_index_sequence(unmatched_probe_indices.len(), null_gather_index(), 0)?;
        let null_build_indices_view = Arc::clone(&null_build_indices).view();
        let unmatched_probe_indices_view = Arc::clone(&unmatched_probe_indices).view();
        let probe_only = gather_join_output(
            build_payload,
            probe_payload,
            null_build_indices_view.inner(),
            unmatched_probe_indices_view.inner(),
            OutOfBoundsPolicy::Nullify,
            OutOfBoundsPolicy::DontCheck,
        )?;

        concat_join_outputs(matched, probe_only)
    }

    /// Gather build rows not matched by previous recorded probes.
    pub fn unmatched_build_rows(
        &self,
        build_payload: &CuDFTableView,
        probe_payload: &CuDFTableView,
        build_out_cols: Option<&[usize]>,
        probe_out_cols: Option<&[usize]>,
    ) -> Result<CuDFTable, CuDFError> {
        let selected_build_payload = build_out_cols.map(|c| select_cols(build_payload, c));
        let selected_probe_payload = probe_out_cols.map(|c| select_cols(probe_payload, c));
        let unmatched_build_indices = self.unmatched_build_indices()?;
        let null_probe_indices =
            join_index_sequence(unmatched_build_indices.len(), null_gather_index(), 0)?;
        let unmatched_build_indices_view = Arc::clone(&unmatched_build_indices).view();
        let null_probe_indices_view = Arc::clone(&null_probe_indices).view();

        gather_join_output(
            selected_build_payload
                .as_ref()
                .unwrap_or_else(|| build_payload.inner()),
            selected_probe_payload
                .as_ref()
                .unwrap_or_else(|| probe_payload.inner()),
            unmatched_build_indices_view.inner(),
            null_probe_indices_view.inner(),
            OutOfBoundsPolicy::DontCheck,
            OutOfBoundsPolicy::Nullify,
        )
    }

    fn record_matched_build_indices(
        &mut self,
        build_indices: Arc<JoinIndexVector>,
    ) -> Result<(), CuDFError> {
        if self.build_rows == 0 || build_indices.len() == 0 {
            return Ok(());
        }

        let Some(distinct_indices) = distinct_valid_indices(Arc::clone(&build_indices).view())?
        else {
            return Ok(());
        };

        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let matched_build_mask = match &self.matched_build_mask {
            Some(mask) => Arc::clone(mask),
            None => {
                let false_scalar = bool_scalar(false)?;
                let mask = Arc::new(CuDFColumn::new(ffi::make_column_from_scalar(
                    false_scalar.inner(),
                    self.build_rows,
                    stream_ref(&stream)?,
                    resource_ref(&resource)?,
                )?));
                self.matched_build_mask = Some(Arc::clone(&mask));
                mask
            }
        };

        let scatter_indices_view = Arc::clone(&distinct_indices).view();
        let true_scalar = bool_scalar(true)?;
        let source = [true_scalar.inner().as_ptr()];
        let target = CuDFTableView::from_column_views(vec![matched_build_mask.view()])?;
        let updated = CuDFTable::from_inner(ffi::scatter_scalars(
            &source,
            scatter_indices_view.inner(),
            target.inner(),
            stream_ref(&stream)?,
            resource_ref(&resource)?,
        )?);
        self.matched_build_mask = Some(Arc::new(
            updated
                .into_columns()
                .into_iter()
                .next()
                .expect("updated matched build mask has one column"),
        ));
        Ok(())
    }

    fn unmatched_build_indices(&self) -> Result<Arc<CuDFColumn>, CuDFError> {
        let all_build_indices = join_index_sequence(self.build_rows, 0, 1)?;
        let Some(matched_build_mask) = &self.matched_build_mask else {
            return Ok(all_build_indices);
        };

        let stream = ffi::get_default_stream();
        let resource = ffi::get_current_device_resource_ref();
        let false_scalar = bool_scalar(false)?;
        let unmatched_mask = Arc::new(CuDFColumn::new(ffi::binary_operation_col_scalar(
            Arc::clone(matched_build_mask).view().inner(),
            false_scalar.inner(),
            BinaryOperator::Equal as i32,
            &bool_data_type(),
            stream_ref(&stream)?,
            resource_ref(&resource)?,
        )?));
        let all_build_table =
            CuDFTableView::from_column_views(vec![Arc::clone(&all_build_indices).view()])?;
        let unmatched_table = CuDFTable::from_inner(ffi::apply_boolean_mask(
            all_build_table.inner(),
            Arc::clone(&unmatched_mask).view().inner(),
            stream_ref(&stream)?,
            resource_ref(&resource)?,
        )?);
        Ok(Arc::new(
            unmatched_table
                .into_columns()
                .into_iter()
                .next()
                .expect("unmatched build indices has one column"),
        ))
    }
}

/// Perform an inner join on two tables using the specified key columns.
///
/// Returns a table containing all rows that have matching keys in both inputs.
/// Columns from both tables are concatenated: `[left_cols | right_cols]`.
/// Pass `left_out_cols` / `right_out_cols` to gather only a subset of columns.
pub fn inner_join(
    left: &CuDFTableView,
    right: &CuDFTableView,
    left_on: &[usize],
    right_on: &[usize],
    left_out_cols: Option<&[usize]>,
    right_out_cols: Option<&[usize]>,
) -> Result<CuDFTable, CuDFError> {
    let left_keys = select_cols(left, left_on);
    let right_keys = select_cols(right, right_on);
    let left_payload = left_out_cols.map(|c| select_cols(left, c));
    let right_payload = right_out_cols.map(|c| select_cols(right, c));
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let indices = ffi::inner_join_indices(
        &left_keys,
        &right_keys,
        NullEquality::Equal as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?;
    gather_join_indices(
        indices,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
        OutOfBoundsPolicy::DontCheck,
        OutOfBoundsPolicy::DontCheck,
    )
}

/// Perform a left outer join on two tables.
///
/// Returns a table with all rows from the left input. Unmatched right rows
/// produce nulls in the right columns.
/// Pass `left_out_cols` / `right_out_cols` to gather only a subset of columns.
pub fn left_join(
    left: &CuDFTableView,
    right: &CuDFTableView,
    left_on: &[usize],
    right_on: &[usize],
    left_out_cols: Option<&[usize]>,
    right_out_cols: Option<&[usize]>,
) -> Result<CuDFTable, CuDFError> {
    let left_keys = select_cols(left, left_on);
    let right_keys = select_cols(right, right_on);
    let left_payload = left_out_cols.map(|c| select_cols(left, c));
    let right_payload = right_out_cols.map(|c| select_cols(right, c));
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let indices = ffi::left_join_indices(
        &left_keys,
        &right_keys,
        NullEquality::Equal as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?;
    gather_join_indices(
        indices,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
        OutOfBoundsPolicy::DontCheck,
        OutOfBoundsPolicy::Nullify,
    )
}

/// Perform a full outer join on two tables.
///
/// Returns all rows from both inputs. Unmatched rows produce nulls on the
/// opposite side.
/// Pass `left_out_cols` / `right_out_cols` to gather only a subset of columns.
pub fn full_join(
    left: &CuDFTableView,
    right: &CuDFTableView,
    left_on: &[usize],
    right_on: &[usize],
    left_out_cols: Option<&[usize]>,
    right_out_cols: Option<&[usize]>,
) -> Result<CuDFTable, CuDFError> {
    let left_keys = select_cols(left, left_on);
    let right_keys = select_cols(right, right_on);
    let left_payload = left_out_cols.map(|c| select_cols(left, c));
    let right_payload = right_out_cols.map(|c| select_cols(right, c));
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let indices = ffi::full_join_indices(
        &left_keys,
        &right_keys,
        NullEquality::Equal as i32,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?;
    gather_join_indices(
        indices,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
        OutOfBoundsPolicy::Nullify,
        OutOfBoundsPolicy::Nullify,
    )
}

/// Perform a left semi join - return only left rows that have at least one match.
///
/// Only left columns are included in the output.
pub fn left_semi_join(
    left: &CuDFTableView,
    right: &CuDFTableView,
    left_on: &[usize],
    right_on: &[usize],
) -> Result<CuDFTable, CuDFError> {
    let left_keys = select_cols(left, left_on);
    let right_keys = select_cols(right, right_on);
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let join = ffi::filtered_join_create(
        &right_keys,
        NullEquality::Equal as i32,
        SetAsBuildTable::Right as i32,
        stream_ref(&stream)?,
    )?;
    let indices = Arc::new(JoinIndexVector::new(ffi::filtered_join_semi_join(
        &join,
        &left_keys,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?));
    let indices_view = indices.view();
    Ok(CuDFTable::from_inner(ffi::gather(
        left.inner(),
        indices_view.inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?))
}

/// Perform a left anti join - return only left rows that have no match.
///
/// Only left columns are included in the output.
pub fn left_anti_join(
    left: &CuDFTableView,
    right: &CuDFTableView,
    left_on: &[usize],
    right_on: &[usize],
) -> Result<CuDFTable, CuDFError> {
    let left_keys = select_cols(left, left_on);
    let right_keys = select_cols(right, right_on);
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    let join = ffi::filtered_join_create(
        &right_keys,
        NullEquality::Equal as i32,
        SetAsBuildTable::Right as i32,
        stream_ref(&stream)?,
    )?;
    let indices = Arc::new(JoinIndexVector::new(ffi::filtered_join_anti_join(
        &join,
        &left_keys,
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?));
    let indices_view = indices.view();
    Ok(CuDFTable::from_inner(ffi::gather(
        left.inner(),
        indices_view.inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?))
}

/// Perform a cross join (Cartesian product) of two tables.
///
/// Returns all combinations of rows from both inputs.
pub fn cross_join(left: &CuDFTableView, right: &CuDFTableView) -> Result<CuDFTable, CuDFError> {
    let stream = ffi::get_default_stream();
    let resource = ffi::get_current_device_resource_ref();
    Ok(CuDFTable::from_inner(ffi::cross_join(
        left.inner(),
        right.inner(),
        stream_ref(&stream)?,
        resource_ref(&resource)?,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CuDFAstOperator, CuDFAstTableReference};
    use arrow::array::{Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_table(keys: Vec<i32>, vals: Vec<i32>) -> CuDFTable {
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
        )
        .unwrap();
        CuDFTable::from_arrow_host(batch).unwrap()
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
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i))
                }
            })
            .collect()
    }

    fn value_less_predicate() -> Result<CuDFAstExpression, CuDFError> {
        let mut predicate = CuDFAstExpression::new();
        let build_value = predicate.column_reference(1, CuDFAstTableReference::Left)?;
        let probe_value = predicate.column_reference(1, CuDFAstTableReference::Right)?;
        predicate.binary_operation(CuDFAstOperator::Less, build_value, probe_value)?;
        Ok(predicate)
    }

    fn filtered_join_args<'a>(
        probe_view: &'a CuDFTableView,
        predicate: &'a CuDFAstExpression,
        build_view: &'a CuDFTableView,
    ) -> CuDFFilteredHashJoinArgs<'a> {
        CuDFFilteredHashJoinArgs {
            probe: probe_view,
            probe_on: &[0],
            build_conditional: build_view,
            probe_conditional: probe_view,
            predicate,
            build_payload: build_view,
            probe_payload: probe_view,
            build_out_cols: Some(&[1]),
            probe_out_cols: Some(&[1]),
        }
    }

    #[test]
    fn test_inner_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500]);

        let result = inner_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            None,
            None,
        )?;
        assert_eq!(result.num_rows(), 2); // keys 2 and 3 match
        assert_eq!(result.num_columns(), 4); // left.key, left.val, right.key, right.val
        Ok(())
    }

    #[test]
    fn test_left_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3], vec![10, 20, 30]);
        let right = make_table(vec![2, 4], vec![200, 400]);

        let result = left_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            None,
            None,
        )?;
        assert_eq!(result.num_rows(), 3); // all 3 left rows
        assert_eq!(result.num_columns(), 4);
        Ok(())
    }

    #[test]
    fn test_full_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2], vec![10, 20]);
        let right = make_table(vec![2, 3], vec![200, 300]);

        let result = full_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            None,
            None,
        )?;
        assert_eq!(result.num_rows(), 3); // key 2 matches, key 1 left-only, key 3 right-only
        assert_eq!(result.num_columns(), 4);
        Ok(())
    }

    #[test]
    fn test_full_join_column_subset_nulls() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2], vec![10, 20]);
        let right = make_table(vec![2, 3], vec![200, 300]);

        let result = full_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            Some(&[1]),
            Some(&[1]),
        )?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);

        let batch = result.into_view().to_arrow_host()?;
        let left_vals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..left_vals.len())
                .filter(|&i| left_vals.is_null(i))
                .count(),
            1
        );
        assert_eq!(
            (0..right_vals.len())
                .filter(|&i| right_vals.is_null(i))
                .count(),
            1
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_column_subset() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500]);

        // Select only left.val (col 1) and right.val (col 1) -> drop both key columns.
        let result = inner_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            Some(&[1]),
            Some(&[1]),
        )?;

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2); // only val from each side

        let batch = result.into_view().to_arrow_host()?;
        let left_vals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut pairs: Vec<(i32, i32)> = (0..left_vals.len())
            .map(|i| (left_vals.value(i), right_vals.value(i)))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![(20, 200), (30, 300)]);
        Ok(())
    }

    #[test]
    fn test_hash_join_inner_join_multiple_probes() -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30]));
        let build_view = Arc::clone(&build).view();
        let join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe_a = make_table(vec![2], vec![200]);
        let probe_a_view = probe_a.into_view();
        let result_a = join.inner_join(
            &probe_a_view,
            &[0],
            &build_view,
            &probe_a_view,
            Some(&[1]),
            Some(&[1]),
        )?;

        let probe_b = make_table(vec![3], vec![300]);
        let probe_b_view = probe_b.into_view();
        let result_b = join.inner_join(
            &probe_b_view,
            &[0],
            &build_view,
            &probe_b_view,
            Some(&[1]),
            Some(&[1]),
        )?;

        assert_eq!(result_a.num_rows(), 1);
        assert_eq!(result_b.num_rows(), 1);
        assert_eq!(result_a.num_columns(), 2);
        assert_eq!(result_b.num_columns(), 2);

        let batch_a = result_a.into_view().to_arrow_host()?;
        let batch_b = result_b.into_view().to_arrow_host()?;
        let left_a = batch_a
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_a = batch_a
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let left_b = batch_b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_b = batch_b
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!((left_a.value(0), right_a.value(0)), (20, 200));
        assert_eq!((left_b.value(0), right_b.value(0)), (30, 300));
        Ok(())
    }

    #[test]
    fn test_gather_filtered_hash_join_indices() -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 2, 3], vec![10, 20, 25, 30]));
        let build_view = Arc::clone(&build).view();
        let join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![2, 2, 3], vec![15, 30, 35]);
        let probe_view = probe.into_view();

        let mut predicate = CuDFAstExpression::new();
        let build_value = predicate.column_reference(0, CuDFAstTableReference::Left)?;
        let probe_value = predicate.column_reference(0, CuDFAstTableReference::Right)?;
        let five = predicate.literal(int32_scalar(5)?)?;
        let build_plus_five =
            predicate.binary_operation(CuDFAstOperator::Add, build_value, five)?;
        predicate.binary_operation(CuDFAstOperator::LessEqual, build_plus_five, probe_value)?;

        let build_values = select_cols(&build_view, &[1]);
        let probe_values = select_cols(&probe_view, &[1]);
        let build_values_view = CuDFTableView::new_with_ref(build_values, build_view._ref.clone());
        let probe_values_view = CuDFTableView::new_with_ref(probe_values, probe_view._ref.clone());
        let result = join.inner_join_filtered(CuDFFilteredHashJoinArgs {
            probe: &probe_view,
            probe_on: &[0],
            build_conditional: &build_values_view,
            probe_conditional: &probe_values_view,
            predicate: &predicate,
            build_payload: &build_view,
            probe_payload: &probe_view,
            build_out_cols: Some(&[1]),
            probe_out_cols: Some(&[1]),
        })?;

        let batch = result.into_view().to_arrow_host()?;
        let left_vals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut pairs: Vec<_> = (0..batch.num_rows())
            .map(|i| (left_vals.value(i), right_vals.value(i)))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![(20, 30), (25, 30), (30, 35)]);
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_empty_predicate_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![2], vec![20]));
        let build_view = Arc::clone(&build).view();
        let join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![2], vec![25]);
        let probe_view = probe.into_view();
        let predicate = CuDFAstExpression::new();
        let result =
            join.inner_join_filtered(filtered_join_args(&probe_view, &predicate, &build_view));

        assert!(result
            .err()
            .is_some_and(|err| err.to_string().contains("empty AST predicate")));
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_records_only_passing_build_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![2, 3], vec![25, 25]);
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;
        let matched = join.inner_join_filtered_and_record_matches(filtered_join_args(
            &probe_view,
            &predicate,
            &build_view,
        ))?;

        let matched_batch = matched.into_view().to_arrow_host()?;
        assert_eq!(int32_values(&matched_batch, 0), vec![20]);
        assert_eq!(int32_values(&matched_batch, 1), vec![25]);

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_view, Some(&[1]), Some(&[1]))?;
        let unmatched_batch = unmatched.into_view().to_arrow_host()?;
        let right_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10, 30]);
        assert_eq!(right_vals.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_duplicate_with_passing_match_is_not_unmatched(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![2], vec![20]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![2, 2], vec![15, 25]);
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;
        let matched = join.inner_join_filtered_and_record_matches(filtered_join_args(
            &probe_view,
            &predicate,
            &build_view,
        ))?;
        assert_eq!(matched.num_rows(), 1);

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_view, Some(&[1]), Some(&[1]))?;
        assert_eq!(unmatched.num_rows(), 0);
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_no_predicate_matches_all_build_rows_unmatched(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2], vec![10, 20]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![1, 2], vec![5, 15]);
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;
        let matched = join.inner_join_filtered_and_record_matches(filtered_join_args(
            &probe_view,
            &predicate,
            &build_view,
        ))?;
        assert_eq!(matched.num_rows(), 0);

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_view, Some(&[1]), Some(&[1]))?;
        let unmatched_batch = unmatched.into_view().to_arrow_host()?;
        let right_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10, 20]);
        assert_eq!(right_vals.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_hash_join_filtered_full_probe_outputs_probe_only_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 2, 3], vec![10, 20, 30, 40]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe = make_table(vec![2, 2, 4], vec![25, 35, 400]);
        let probe_view = probe.into_view();
        let predicate = value_less_predicate()?;
        let result = join.probe_left_join_filtered_and_record_matches(filtered_join_args(
            &probe_view,
            &predicate,
            &build_view,
        ))?;

        let batch = result.into_view().to_arrow_host()?;
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

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_view, Some(&[1]), Some(&[1]))?;
        let unmatched_batch = unmatched.into_view().to_arrow_host()?;
        let right_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10, 40]);
        assert_eq!(right_vals.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_hash_join_records_unmatched_build_rows() -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3], vec![10, 20, 30]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe_a = make_table(vec![2], vec![200]);
        let probe_a_view = probe_a.into_view();
        let inner = join.inner_join_and_record_matches(
            &probe_a_view,
            &[0],
            &build_view,
            &probe_a_view,
            Some(&[1]),
            Some(&[1]),
        )?;
        let inner_batch = inner.into_view().to_arrow_host()?;
        assert_eq!(int32_values(&inner_batch, 0), vec![20]);
        assert_eq!(int32_values(&inner_batch, 1), vec![200]);

        let probe_b = make_table(vec![3, 4], vec![300, 400]);
        let probe_b_view = probe_b.into_view();
        let probe_left = join.probe_left_join_and_record_matches(
            &probe_b_view,
            &[0],
            &build_view,
            &probe_b_view,
            Some(&[1]),
            Some(&[1]),
        )?;
        assert_eq!(probe_left.num_rows(), 2);
        assert_eq!(probe_left.num_columns(), 2);

        let probe_left_batch = probe_left.into_view().to_arrow_host()?;
        let build_vals = probe_left_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(build_vals.null_count(), 1);
        assert_eq!(int32_values(&probe_left_batch, 1), vec![300, 400]);

        let non_null_build_vals: Vec<_> = (0..build_vals.len())
            .filter(|&i| build_vals.is_valid(i))
            .map(|i| build_vals.value(i))
            .collect();
        assert_eq!(non_null_build_vals, vec![30]);

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_b_view, Some(&[1]), Some(&[1]))?;
        assert_eq!(unmatched.num_rows(), 1);
        assert_eq!(unmatched.num_columns(), 2);

        let unmatched_batch = unmatched.into_view().to_arrow_host()?;
        let unmatched_probe_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10]);
        assert_eq!(unmatched_probe_vals.null_count(), 1);
        Ok(())
    }

    #[test]
    fn test_hash_join_match_mask_handles_duplicate_and_unmatched_probe_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe_a = make_table(vec![2, 2], vec![200, 201]);
        let probe_a_view = probe_a.into_view();
        let inner = join.inner_join_and_record_matches(
            &probe_a_view,
            &[0],
            &build_view,
            &probe_a_view,
            Some(&[1]),
            Some(&[1]),
        )?;
        assert_eq!(inner.num_rows(), 2);

        let probe_b = make_table(vec![2, 3, 3, 5], vec![202, 300, 301, 500]);
        let probe_b_view = probe_b.into_view();
        let probe_left = join.probe_left_join_and_record_matches(
            &probe_b_view,
            &[0],
            &build_view,
            &probe_b_view,
            Some(&[1]),
            Some(&[1]),
        )?;
        assert_eq!(probe_left.num_rows(), 4);

        let probe_left_batch = probe_left.into_view().to_arrow_host()?;
        let build_vals = probe_left_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(build_vals.null_count(), 1);

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_b_view, Some(&[1]), Some(&[1]))?;
        assert_eq!(unmatched.num_rows(), 2);
        assert_eq!(unmatched.num_columns(), 2);

        let unmatched_batch = unmatched.into_view().to_arrow_host()?;
        let unmatched_probe_vals = unmatched_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int32_values(&unmatched_batch, 0), vec![10, 40]);
        assert_eq!(unmatched_probe_vals.null_count(), 2);
        Ok(())
    }

    #[test]
    fn test_hash_join_match_mask_handles_all_build_rows_matched(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let build = Arc::new(make_table(vec![1, 2], vec![10, 20]));
        let build_view = Arc::clone(&build).view();
        let mut join = CuDFHashJoin::try_new(&build_view, &[0], CuDFNullEquality::Unequal)?;

        let probe_a = make_table(vec![1, 1], vec![100, 101]);
        let probe_a_view = probe_a.into_view();
        join.inner_join_and_record_matches(
            &probe_a_view,
            &[0],
            &build_view,
            &probe_a_view,
            Some(&[1]),
            Some(&[1]),
        )?;

        let probe_b = make_table(vec![2, 2], vec![200, 201]);
        let probe_b_view = probe_b.into_view();
        join.inner_join_and_record_matches(
            &probe_b_view,
            &[0],
            &build_view,
            &probe_b_view,
            Some(&[1]),
            Some(&[1]),
        )?;

        let unmatched =
            join.unmatched_build_rows(&build_view, &probe_b_view, Some(&[1]), Some(&[1]))?;
        assert_eq!(unmatched.num_rows(), 0);
        assert_eq!(unmatched.num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_left_join_column_subset_nulls() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3], vec![10, 20, 30]);
        let right = make_table(vec![2, 4], vec![200, 400]);

        // Select only right.val (col 1) -> left side gets all columns.
        let result = left_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            None,
            Some(&[1]),
        )?;

        assert_eq!(result.num_rows(), 3); // all left rows preserved
        assert_eq!(result.num_columns(), 3); // left.key, left.val, right.val

        let batch = result.into_view().to_arrow_host()?;
        let right_val_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // key=1 and key=3 have no match -> right.val must be null for those rows.
        let nulls: usize = (0..right_val_col.len())
            .filter(|&i| right_val_col.is_null(i))
            .count();
        assert_eq!(nulls, 2);
        Ok(())
    }

    #[test]
    fn test_left_semi_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500]);

        let result = left_semi_join(&left.into_view(), &right.into_view(), &[0], &[0])?;
        assert_eq!(result.num_rows(), 2); // keys 2 and 3 match
        assert_eq!(result.num_columns(), 2); // only left columns

        let batch = result.into_view().to_arrow_host()?;
        let mut keys = int32_values(&batch, 0);
        keys.sort();
        assert_eq!(keys, vec![2, 3]);
        Ok(())
    }

    #[test]
    fn test_left_anti_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500]);

        let result = left_anti_join(&left.into_view(), &right.into_view(), &[0], &[0])?;
        assert_eq!(result.num_rows(), 2); // keys 1 and 4 have no match
        assert_eq!(result.num_columns(), 2); // only left columns

        let batch = result.into_view().to_arrow_host()?;
        let mut keys = int32_values(&batch, 0);
        keys.sort();
        assert_eq!(keys, vec![1, 4]);
        Ok(())
    }

    #[test]
    fn test_cross_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2], vec![10, 20]);
        let right = make_table(vec![3, 4, 5], vec![30, 40, 50]);

        let result = cross_join(&left.into_view(), &right.into_view())?;
        assert_eq!(result.num_rows(), 6); // 2 * 3 = 6
        assert_eq!(result.num_columns(), 4);
        Ok(())
    }

    #[test]
    fn test_inner_join_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3], vec![10, 20, 30]);
        let right = make_table(vec![4, 5, 6], vec![40, 50, 60]);

        let result = inner_join(
            &left.into_view(),
            &right.into_view(),
            &[0],
            &[0],
            None,
            None,
        )?;
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 4);
        Ok(())
    }
}
