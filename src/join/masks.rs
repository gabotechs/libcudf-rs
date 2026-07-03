use crate::data_type::arrow_type_to_cudf_data_type;
use crate::execution_policy::OperationLaunch;
use crate::keep_alive::CuDFKeepAlive;
use crate::{CuDFColumn, CuDFColumnView, CuDFError, CuDFScalar, CuDFTable, CuDFTableView};
use arrow::array::{BooleanArray, Int32Array, Scalar};
use arrow_schema::{ArrowError, DataType};
use cxx::UniquePtr;
use libcudf_sys::{ffi, BinaryOperator, DuplicateKeepOption, NanEquality, NullEquality};
use std::sync::Arc;

use super::indices::JoinIndexVector;

pub(super) fn distinct_valid_indices(
    launch: &mut OperationLaunch<'_>,
    indices: CuDFColumnView,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    if indices.inner().size() == 0 {
        return join_index_sequence(launch, 0, 0, 1);
    }

    let zero = int32_scalar_on(launch, 0)?;
    launch.wait_column(&indices)?;
    launch.wait_scalar(&zero)?;
    let bool_type = boolean_cudf_type()?;
    let valid_mask = CuDFColumn::from_inner(ffi::binary_operation_col_scalar(
        indices.inner(),
        zero.inner(),
        BinaryOperator::GreaterEqual as i32,
        &bool_type,
        launch.stream()?,
        launch.resource(),
    )?);
    let valid_mask_dependency = launch.record_stream_dependency(vec![
        CuDFKeepAlive::ColumnView {
            _column: indices.clone(),
        },
        CuDFKeepAlive::Scalar {
            _scalar: zero.clone(),
        },
    ])?;
    let valid_mask = Arc::new(valid_mask.with_stream_readiness(valid_mask_dependency));

    let valid_indices = apply_mask_to_indices(launch, indices, valid_mask)?;
    if valid_indices.is_empty() {
        return Ok(valid_indices);
    }

    let valid_indices_view = Arc::clone(&valid_indices).view();
    let valid_indices_table = CuDFTableView::from_column_views(vec![valid_indices_view])?;
    launch.wait_table(&valid_indices_table)?;
    let distinct_indices = CuDFTable::from_inner(ffi::distinct(
        valid_indices_table.inner(),
        &[0],
        DuplicateKeepOption::KeepAny as i32,
        NullEquality::Equal as i32,
        NanEquality::AllEqual as i32,
        launch.stream()?,
        launch.resource(),
    )?);
    let distinct_dependency = launch.record_stream_dependency(vec![CuDFKeepAlive::TableView {
        _table: valid_indices_table,
    }])?;
    Ok(Arc::new(
        single_column(distinct_indices, "distinct indices")?
            .with_stream_readiness(distinct_dependency),
    ))
}

pub(super) fn false_mask(
    launch: &mut OperationLaunch<'_>,
    row_count: usize,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let false_scalar = bool_scalar_on(launch, false)?;
    launch.wait_scalar(&false_scalar)?;
    let mask = CuDFColumn::from_inner(ffi::make_column_from_scalar(
        false_scalar.inner(),
        row_count,
        launch.stream()?,
        launch.resource(),
    )?);
    let dependency = launch.record_stream_dependency(vec![CuDFKeepAlive::Scalar {
        _scalar: false_scalar,
    }])?;
    Ok(Arc::new(mask.with_stream_readiness(dependency)))
}

pub(super) fn scatter_true_into_mask(
    launch: &mut OperationLaunch<'_>,
    mask: Arc<CuDFColumn>,
    indices: Arc<CuDFColumn>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let indices_view = Arc::clone(&indices).view();
    launch.wait_column(&indices_view)?;
    let true_scalar = bool_scalar_on(launch, true)?;
    launch.wait_scalar(&true_scalar)?;
    let mask_view = Arc::clone(&mask).view();
    let target = CuDFTableView::from_column_views(vec![mask_view])?;
    launch.wait_table(&target)?;
    let source = [true_scalar.inner().as_ptr()];
    let updated = CuDFTable::from_inner(ffi::scatter_scalars(
        &source,
        indices_view.inner(),
        target.inner(),
        launch.stream()?,
        launch.resource(),
    )?);
    let dependency = launch.record_stream_dependency(vec![
        CuDFKeepAlive::ColumnView {
            _column: indices_view,
        },
        CuDFKeepAlive::Scalar {
            _scalar: true_scalar,
        },
        CuDFKeepAlive::TableView { _table: target },
    ])?;
    Ok(Arc::new(
        single_column(updated, "updated mask")?.with_stream_readiness(dependency),
    ))
}

pub(super) fn unmatched_indices_from_matches(
    launch: &mut OperationLaunch<'_>,
    row_count: usize,
    matched_indices: Arc<JoinIndexVector>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let mask = false_mask(launch, row_count)?;
    let distinct_indices = distinct_valid_indices(launch, Arc::clone(&matched_indices).view())?;
    let matched_mask = scatter_true_into_mask(launch, mask, distinct_indices)?;
    row_indices_where_mask_is_false(launch, row_count, matched_mask)
}

pub(super) fn row_indices_where_mask_is_false(
    launch: &mut OperationLaunch<'_>,
    row_count: usize,
    mask: Arc<CuDFColumn>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let all_indices = join_index_sequence(launch, row_count, 0, 1)?;
    let is_false = false_value_mask(launch, mask)?;
    apply_mask_to_indices(launch, Arc::clone(&all_indices).view(), is_false)
}

pub(super) fn join_index_sequence(
    launch: &mut OperationLaunch<'_>,
    size: usize,
    init: i32,
    step: i32,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let init_scalar = int32_scalar_on(launch, init)?;
    let step_scalar = int32_scalar_on(launch, step)?;
    launch.wait_scalar(&init_scalar)?;
    launch.wait_scalar(&step_scalar)?;
    let column = CuDFColumn::from_inner(ffi::sequence(
        size,
        init_scalar.inner(),
        step_scalar.inner(),
        launch.stream()?,
        launch.resource(),
    )?);
    let dependency = launch.record_stream_dependency(vec![
        CuDFKeepAlive::Scalar {
            _scalar: init_scalar,
        },
        CuDFKeepAlive::Scalar {
            _scalar: step_scalar,
        },
    ])?;
    Ok(Arc::new(column.with_stream_readiness(dependency)))
}

fn false_value_mask(
    launch: &mut OperationLaunch<'_>,
    mask: Arc<CuDFColumn>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let false_scalar = bool_scalar_on(launch, false)?;
    let mask_view = Arc::clone(&mask).view();
    launch.wait_column(&mask_view)?;
    launch.wait_scalar(&false_scalar)?;
    let bool_type = boolean_cudf_type()?;
    let is_false = CuDFColumn::from_inner(ffi::binary_operation_col_scalar(
        mask_view.inner(),
        false_scalar.inner(),
        BinaryOperator::Equal as i32,
        &bool_type,
        launch.stream()?,
        launch.resource(),
    )?);
    let dependency = launch.record_stream_dependency(vec![
        CuDFKeepAlive::ColumnView { _column: mask_view },
        CuDFKeepAlive::Scalar {
            _scalar: false_scalar,
        },
    ])?;
    Ok(Arc::new(is_false.with_stream_readiness(dependency)))
}

fn apply_mask_to_indices(
    launch: &mut OperationLaunch<'_>,
    indices: CuDFColumnView,
    mask: Arc<CuDFColumn>,
) -> Result<Arc<CuDFColumn>, CuDFError> {
    let indices_table = CuDFTableView::from_column_views(vec![indices])?;
    launch.wait_table(&indices_table)?;
    let mask_view = Arc::clone(&mask).view();
    launch.wait_column(&mask_view)?;
    let filtered_table = CuDFTable::from_inner(ffi::apply_boolean_mask(
        indices_table.inner(),
        mask_view.inner(),
        launch.stream()?,
        launch.resource(),
    )?);
    let dependency = launch.record_stream_dependency(vec![
        CuDFKeepAlive::TableView {
            _table: indices_table,
        },
        CuDFKeepAlive::ColumnView { _column: mask_view },
    ])?;
    Ok(Arc::new(
        single_column(filtered_table, "filtered indices")?.with_stream_readiness(dependency),
    ))
}

fn single_column(table: CuDFTable, name: &str) -> Result<CuDFColumn, CuDFError> {
    table.into_columns().into_iter().next().ok_or_else(|| {
        CuDFError::ArrowError(ArrowError::InvalidArgumentError(format!(
            "{name} expected one output column"
        )))
    })
}

fn boolean_cudf_type() -> Result<UniquePtr<ffi::DataType>, CuDFError> {
    arrow_type_to_cudf_data_type(&DataType::Boolean).ok_or_else(|| {
        CuDFError::ArrowError(ArrowError::NotYetImplemented(
            "Boolean type is not supported by cuDF".to_string(),
        ))
    })
}

fn int32_scalar_on(launch: &OperationLaunch<'_>, value: i32) -> Result<CuDFScalar, CuDFError> {
    let array = Int32Array::from(vec![value]);
    launch
        .context()
        .execute(CuDFScalar::from_arrow_host(Scalar::new(&array)))
}

fn bool_scalar_on(launch: &OperationLaunch<'_>, value: bool) -> Result<CuDFScalar, CuDFError> {
    let array = BooleanArray::from(vec![value]);
    launch
        .context()
        .execute(CuDFScalar::from_arrow_host(Scalar::new(&array)))
}
