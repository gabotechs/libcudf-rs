use crate::execution_policy::OperationLaunch;
use crate::keep_alive::CuDFKeepAlive;
use crate::{CuDFAstExpression, CuDFError, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::{ffi, JoinKind, OutOfBoundsPolicy};
use std::sync::Arc;

use super::indices::{split_hash_join_indices, split_join_indices, JoinIndexVector};

pub(super) struct GatherJoinOutputArgs<'a> {
    pub(super) left_payload: &'a ffi::TableView,
    pub(super) right_payload: &'a ffi::TableView,
    pub(super) left_indices: &'a ffi::ColumnView,
    pub(super) right_indices: &'a ffi::ColumnView,
    pub(super) left_policy: OutOfBoundsPolicy,
    pub(super) right_policy: OutOfBoundsPolicy,
}

pub(super) fn gather_join_output(
    launch: &mut OperationLaunch<'_>,
    args: GatherJoinOutputArgs<'_>,
) -> Result<CuDFTable, CuDFError> {
    let left = CuDFTable::from_inner(ffi::gather_with_policy(
        args.left_payload,
        args.left_indices,
        args.left_policy as i32,
        launch.stream()?,
        launch.resource(),
    )?);
    let right = CuDFTable::from_inner(ffi::gather_with_policy(
        args.right_payload,
        args.right_indices,
        args.right_policy as i32,
        launch.stream()?,
        launch.resource(),
    )?);
    let mut columns = left.into_columns();
    columns.extend(right.into_columns());
    Ok(CuDFTable::from_owned_columns(columns))
}

pub(super) fn gather_join_indices(
    launch: &mut OperationLaunch<'_>,
    indices: UniquePtr<ffi::JoinIndices>,
    left_payload: &ffi::TableView,
    right_payload: &ffi::TableView,
    left_policy: OutOfBoundsPolicy,
    right_policy: OutOfBoundsPolicy,
) -> Result<CuDFTable, CuDFError> {
    let (left_indices, right_indices) = split_join_indices(indices);
    let left_indices_view = Arc::clone(&left_indices).view();
    let right_indices_view = Arc::clone(&right_indices).view();
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&left_indices),
    });
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&right_indices),
    });
    gather_join_output(
        launch,
        GatherJoinOutputArgs {
            left_payload,
            right_payload,
            left_indices: left_indices_view.inner(),
            right_indices: right_indices_view.inner(),
            left_policy,
            right_policy,
        },
    )
}

pub(super) fn gather_hash_join_indices(
    launch: &mut OperationLaunch<'_>,
    indices: UniquePtr<ffi::HashJoinIndices>,
    build_payload: &ffi::TableView,
    probe_payload: &ffi::TableView,
    build_policy: OutOfBoundsPolicy,
    probe_policy: OutOfBoundsPolicy,
) -> Result<(CuDFTable, Arc<JoinIndexVector>), CuDFError> {
    let (build_indices, probe_indices) = split_hash_join_indices(indices);
    let build_indices_view = Arc::clone(&build_indices).view();
    let probe_indices_view = Arc::clone(&probe_indices).view();
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&build_indices),
    });
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&probe_indices),
    });
    let result = gather_join_output(
        launch,
        GatherJoinOutputArgs {
            left_payload: build_payload,
            right_payload: probe_payload,
            left_indices: build_indices_view.inner(),
            right_indices: probe_indices_view.inner(),
            left_policy: build_policy,
            right_policy: probe_policy,
        },
    )?;
    Ok((result, build_indices))
}

pub(super) struct FilteredHashJoinIndicesArgs<'a> {
    pub(super) build_conditional: &'a ffi::TableView,
    pub(super) probe_conditional: &'a ffi::TableView,
    pub(super) predicate: &'a CuDFAstExpression,
    pub(super) join_kind: JoinKind,
    pub(super) build_payload: &'a ffi::TableView,
    pub(super) probe_payload: &'a ffi::TableView,
    pub(super) build_policy: OutOfBoundsPolicy,
    pub(super) probe_policy: OutOfBoundsPolicy,
}

pub(super) fn gather_filtered_hash_join_indices(
    launch: &mut OperationLaunch<'_>,
    indices: UniquePtr<ffi::HashJoinIndices>,
    args: FilteredHashJoinIndicesArgs<'_>,
) -> Result<(CuDFTable, Arc<JoinIndexVector>, Arc<JoinIndexVector>), CuDFError> {
    let (build_indices, probe_indices) = split_hash_join_indices(indices);
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&build_indices),
    });
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&probe_indices),
    });
    launch.keep_alive(CuDFKeepAlive::AstExpression {
        _expression: args.predicate.clone(),
    });

    let filtered_indices = ffi::filter_join_indices(
        args.build_conditional,
        args.probe_conditional,
        build_indices.as_sys()?,
        probe_indices.as_sys()?,
        args.predicate.inner(),
        args.join_kind as i32,
        launch.stream()?,
        launch.resource(),
    )?;
    let (filtered_build_indices, filtered_probe_indices) = split_join_indices(filtered_indices);
    let filtered_build_indices_view = Arc::clone(&filtered_build_indices).view();
    let filtered_probe_indices_view = Arc::clone(&filtered_probe_indices).view();
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&filtered_build_indices),
    });
    launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
        _indices: Arc::clone(&filtered_probe_indices),
    });

    let result = gather_join_output(
        launch,
        GatherJoinOutputArgs {
            left_payload: args.build_payload,
            right_payload: args.probe_payload,
            left_indices: filtered_build_indices_view.inner(),
            right_indices: filtered_probe_indices_view.inner(),
            left_policy: args.build_policy,
            right_policy: args.probe_policy,
        },
    )?;
    Ok((result, filtered_build_indices, filtered_probe_indices))
}

pub(super) fn concat_join_outputs(
    launch: &mut OperationLaunch<'_>,
    first: CuDFTable,
    second: CuDFTable,
) -> Result<CuDFTable, CuDFError> {
    if first.num_rows() == 0 {
        return Ok(second);
    }
    if second.num_rows() == 0 {
        return Ok(first);
    }

    let views = vec![first.into_view(), second.into_view()];
    let mut inner_views = Vec::with_capacity(views.len());
    for view in views {
        launch.wait_table(&view)?;
        inner_views.push(view.into_inner());
    }
    Ok(CuDFTable::from_inner(ffi::concat_table_views(
        &inner_views,
        launch.stream()?,
        launch.resource(),
    )?))
}
