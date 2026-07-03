use crate::deferred_operation::operation_impl::CuDFOperationImpl;
use crate::execution_policy;
use crate::{CuDFError, CuDFExecutionContext, CuDFOperation, CuDFTable, CuDFTableView};
use cxx::UniquePtr;
use libcudf_sys::{ffi, OutOfBoundsPolicy, SetAsBuildTable};
use std::sync::Arc;

use super::common::{table_ref, CuDFNullEquality, SelectedPayloads};
use super::indices::JoinIndexVector;
use super::output::gather_join_indices;
use crate::keep_alive::CuDFKeepAlive;

#[derive(Clone, Copy)]
enum EquiJoinKind {
    Inner,
    Left,
    Full,
}

impl EquiJoinKind {
    fn indices(
        self,
        left_keys: &CuDFTableView,
        right_keys: &CuDFTableView,
        null_equality: CuDFNullEquality,
        launch: &execution_policy::OperationLaunch<'_>,
    ) -> Result<cxx::UniquePtr<ffi::JoinIndices>, CuDFError> {
        match self {
            Self::Inner => Ok(ffi::inner_join_indices(
                left_keys.inner(),
                right_keys.inner(),
                null_equality.into_sys() as i32,
                launch.stream()?,
                launch.resource(),
            )?),
            Self::Left => Ok(ffi::left_join_indices(
                left_keys.inner(),
                right_keys.inner(),
                null_equality.into_sys() as i32,
                launch.stream()?,
                launch.resource(),
            )?),
            Self::Full => Ok(ffi::full_join_indices(
                left_keys.inner(),
                right_keys.inner(),
                null_equality.into_sys() as i32,
                launch.stream()?,
                launch.resource(),
            )?),
        }
    }

    fn policies(self) -> (OutOfBoundsPolicy, OutOfBoundsPolicy) {
        match self {
            Self::Inner => (OutOfBoundsPolicy::DontCheck, OutOfBoundsPolicy::DontCheck),
            Self::Left => (OutOfBoundsPolicy::DontCheck, OutOfBoundsPolicy::Nullify),
            Self::Full => (OutOfBoundsPolicy::Nullify, OutOfBoundsPolicy::Nullify),
        }
    }
}

/// Deferred inner, left, or full equi-join.
///
/// Created by [`inner_join`], [`left_join`], or [`full_join`]. Execution waits
/// for both inputs, joins the selected key columns, then gathers output columns
/// from the left table followed by the right table.
///
/// # Errors
///
/// Execution returns an error if a selected column index is out of bounds, the
/// key tables are incompatible, or cuDF cannot allocate the output table.
pub struct EquiJoin<'a> {
    kind: EquiJoinKind,
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
    left_out_cols: Option<&'a [usize]>,
    right_out_cols: Option<&'a [usize]>,
    null_equality: CuDFNullEquality,
}

impl<'a> EquiJoin<'a> {
    /// Set whether null join-key values compare equal.
    ///
    /// Joins default to [`CuDFNullEquality::Equal`].
    pub fn null_equality(mut self, null_equality: CuDFNullEquality) -> Self {
        self.null_equality = null_equality;
        self
    }

    /// Gather only selected columns from the left input.
    ///
    /// Column indices refer to the original left table. By default all left
    /// columns are gathered.
    pub fn select_left(mut self, cols: &'a [usize]) -> Self {
        self.left_out_cols = Some(cols);
        self
    }

    /// Gather only selected columns from the right input.
    ///
    /// Column indices refer to the original right table. By default all right
    /// columns are gathered.
    pub fn select_right(mut self, cols: &'a [usize]) -> Self {
        self.right_out_cols = Some(cols);
        self
    }
}

impl CuDFOperation for EquiJoin<'_> {
    type Output = CuDFTable;
}

impl CuDFOperationImpl for EquiJoin<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        let mut launch = execution_policy::launch(ctx)?;
        let left_keys = self.left.select_columns(self.left_on)?;
        let right_keys = self.right.select_columns(self.right_on)?;
        let payloads = SelectedPayloads::new(
            self.left,
            self.right,
            self.left_out_cols,
            self.right_out_cols,
        )?;
        launch.wait_table(&left_keys)?;
        launch.wait_table(&right_keys)?;
        launch.wait_table(payloads.left_view())?;
        launch.wait_table(payloads.right_view())?;
        let indices = self
            .kind
            .indices(&left_keys, &right_keys, self.null_equality, &launch)?;
        let (left_policy, right_policy) = self.kind.policies();
        let result = gather_join_indices(
            &mut launch,
            indices,
            payloads.left()?,
            payloads.right()?,
            left_policy,
            right_policy,
        )?;
        launch.ready_table(result)
    }
}

#[derive(Clone, Copy)]
enum LeftFilterJoinKind {
    Semi,
    Anti,
}

impl LeftFilterJoinKind {
    fn indices(
        self,
        join: &ffi::FilteredJoin,
        probe_keys: &CuDFTableView,
        launch: &execution_policy::OperationLaunch<'_>,
    ) -> Result<cxx::UniquePtr<ffi::DeviceIndexVector>, CuDFError> {
        match self {
            Self::Semi => Ok(ffi::filtered_join_semi_join(
                join,
                probe_keys.inner(),
                launch.stream()?,
                launch.resource(),
            )?),
            Self::Anti => Ok(ffi::filtered_join_anti_join(
                join,
                probe_keys.inner(),
                launch.stream()?,
                launch.resource(),
            )?),
        }
    }
}

/// Retains a filtered join build with the keys used to construct it.
pub(crate) struct FilteredJoinBuild {
    inner: UniquePtr<ffi::FilteredJoin>,
    _build_keys: CuDFTableView,
}

impl FilteredJoinBuild {
    fn new(inner: UniquePtr<ffi::FilteredJoin>, build_keys: CuDFTableView) -> Self {
        Self {
            inner,
            _build_keys: build_keys,
        }
    }

    fn inner(&self) -> Result<&ffi::FilteredJoin, CuDFError> {
        self.inner
            .as_ref()
            .ok_or(CuDFError::NullHandle("filtered join"))
    }
}

/// Deferred left semi or left anti join.
///
/// Created by [`left_semi_join`] or [`left_anti_join`]. Execution returns rows
/// from the left table only: matching rows for semi joins, and non-matching
/// rows for anti joins.
///
/// # Errors
///
/// Execution returns an error if a selected column index is out of bounds, the
/// key tables are incompatible, or cuDF cannot allocate the output table.
pub struct LeftFilterJoin<'a> {
    kind: LeftFilterJoinKind,
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
    null_equality: CuDFNullEquality,
}

impl LeftFilterJoin<'_> {
    /// Set whether null join-key values compare equal.
    ///
    /// Joins default to [`CuDFNullEquality::Equal`].
    pub fn null_equality(mut self, null_equality: CuDFNullEquality) -> Self {
        self.null_equality = null_equality;
        self
    }
}

impl CuDFOperation for LeftFilterJoin<'_> {
    type Output = CuDFTable;
}

impl CuDFOperationImpl for LeftFilterJoin<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        let mut launch = execution_policy::launch(ctx)?;
        let left_keys = self.left.select_columns(self.left_on)?;
        let right_keys = self.right.select_columns(self.right_on)?;
        launch.wait_table(&left_keys)?;
        launch.wait_table(&right_keys)?;
        launch.wait_table(self.left)?;
        let join = Arc::new(FilteredJoinBuild::new(
            ffi::filtered_join_create(
                right_keys.inner(),
                self.null_equality.into_sys() as i32,
                SetAsBuildTable::Right as i32,
                launch.stream()?,
            )?,
            right_keys,
        ));
        launch.keep_alive(CuDFKeepAlive::FilteredJoinBuild {
            _state: Arc::clone(&join),
        });
        let indices = Arc::new(JoinIndexVector::new(self.kind.indices(
            join.inner()?,
            &left_keys,
            &launch,
        )?));
        let indices_view = Arc::clone(&indices).view();
        launch.keep_alive(CuDFKeepAlive::JoinIndexVector {
            _indices: Arc::clone(&indices),
        });
        let result = CuDFTable::from_inner(ffi::gather(
            self.left.inner(),
            indices_view.inner(),
            launch.stream()?,
            launch.resource(),
        )?);
        launch.ready_table(result)
    }
}

/// Deferred cross join.
///
/// Execution returns the Cartesian product of the input tables, with left
/// columns followed by right columns.
///
/// # Errors
///
/// Execution returns an error if cuDF cannot allocate the output table.
pub struct CrossJoin<'a> {
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
}

impl CuDFOperation for CrossJoin<'_> {
    type Output = CuDFTable;
}

impl CuDFOperationImpl for CrossJoin<'_> {
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        let mut launch = execution_policy::launch(ctx)?;
        launch.wait_table(self.left)?;
        launch.wait_table(self.right)?;
        let inner = ffi::cross_join(
            table_ref(self.left)?,
            table_ref(self.right)?,
            launch.stream()?,
            launch.resource(),
        )?;
        launch.ready_table(CuDFTable::from_inner(inner))
    }
}

/// Create a deferred inner join.
///
/// The output contains rows whose selected left and right key columns match.
/// Left output columns appear before right output columns.
pub fn inner_join<'a>(
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> EquiJoin<'a> {
    equi_join(EquiJoinKind::Inner, left, right, left_on, right_on)
}

/// Create a deferred left join.
///
/// The output preserves every left row and null-fills right columns for rows
/// without a matching right key.
pub fn left_join<'a>(
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> EquiJoin<'a> {
    equi_join(EquiJoinKind::Left, left, right, left_on, right_on)
}

/// Create a deferred full join.
///
/// The output contains matching rows plus unmatched rows from both inputs.
/// Missing columns on either side are null-filled.
pub fn full_join<'a>(
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> EquiJoin<'a> {
    equi_join(EquiJoinKind::Full, left, right, left_on, right_on)
}

/// Create a deferred left semi join.
///
/// The output contains left rows that have at least one matching right key.
pub fn left_semi_join<'a>(
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> LeftFilterJoin<'a> {
    left_filter_join(LeftFilterJoinKind::Semi, left, right, left_on, right_on)
}

/// Create a deferred left anti join.
///
/// The output contains left rows that have no matching right key.
pub fn left_anti_join<'a>(
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> LeftFilterJoin<'a> {
    left_filter_join(LeftFilterJoinKind::Anti, left, right, left_on, right_on)
}

/// Create a deferred cross join.
pub fn cross_join<'a>(left: &'a CuDFTableView, right: &'a CuDFTableView) -> CrossJoin<'a> {
    CrossJoin { left, right }
}

fn equi_join<'a>(
    kind: EquiJoinKind,
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> EquiJoin<'a> {
    EquiJoin {
        kind,
        left,
        right,
        left_on,
        right_on,
        left_out_cols: None,
        right_out_cols: None,
        null_equality: CuDFNullEquality::Equal,
    }
}

fn left_filter_join<'a>(
    kind: LeftFilterJoinKind,
    left: &'a CuDFTableView,
    right: &'a CuDFTableView,
    left_on: &'a [usize],
    right_on: &'a [usize],
) -> LeftFilterJoin<'a> {
    LeftFilterJoin {
        kind,
        left,
        right,
        left_on,
        right_on,
        null_equality: CuDFNullEquality::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_inner_join_selected_columns() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40])?;
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500])?;
        let left_view = left.into_view();
        let right_view = right.into_view();

        let result = crate::execute_cudf(
            inner_join(&left_view, &right_view, &[0], &[0])
                .select_left(&[1])
                .select_right(&[1]),
        )?;

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);
        let batch = crate::execute_cudf(result.into_view().to_arrow_host())?;
        let mut pairs: Vec<_> = int32_values(&batch, 0)
            .into_iter()
            .zip(int32_values(&batch, 1))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![(20, 200), (30, 300)]);
        Ok(())
    }

    #[test]
    fn test_left_join_nullifies_unmatched_right_rows() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3], vec![10, 20, 30])?;
        let right = make_table(vec![2, 4], vec![200, 400])?;
        let left_view = left.into_view();
        let right_view = right.into_view();

        let result =
            crate::execute_cudf(left_join(&left_view, &right_view, &[0], &[0]).select_right(&[1]))?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);
        let batch = crate::execute_cudf(result.into_view().to_arrow_host())?;
        let right_vals = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..right_vals.len())
                .filter(|&i| right_vals.is_null(i))
                .count(),
            2
        );
        Ok(())
    }

    #[test]
    fn test_full_join_selected_columns_nullify_both_sides() -> Result<(), Box<dyn std::error::Error>>
    {
        let left = make_table(vec![1, 2], vec![10, 20])?;
        let right = make_table(vec![2, 3], vec![200, 300])?;
        let left_view = left.into_view();
        let right_view = right.into_view();

        let result = crate::execute_cudf(
            full_join(&left_view, &right_view, &[0], &[0])
                .select_left(&[1])
                .select_right(&[1]),
        )?;

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
        let batch = crate::execute_cudf(result.into_view().to_arrow_host())?;
        for column in [0, 1] {
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!((0..values.len()).filter(|&i| values.is_null(i)).count(), 1);
        }
        Ok(())
    }

    #[test]
    fn test_left_semi_and_anti_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40])?;
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500])?;
        let left_view = left.into_view();
        let right_view = right.into_view();

        let semi = crate::execute_cudf(left_semi_join(&left_view, &right_view, &[0], &[0]))?;
        let anti = crate::execute_cudf(left_anti_join(&left_view, &right_view, &[0], &[0]))?;

        let semi_batch = crate::execute_cudf(semi.into_view().to_arrow_host())?;
        let anti_batch = crate::execute_cudf(anti.into_view().to_arrow_host())?;
        let mut semi_keys = int32_values(&semi_batch, 0);
        let mut anti_keys = int32_values(&anti_batch, 0);
        semi_keys.sort();
        anti_keys.sort();
        assert_eq!(semi_keys, vec![2, 3]);
        assert_eq!(anti_keys, vec![1, 4]);
        Ok(())
    }

    #[test]
    fn test_cross_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2], vec![10, 20])?;
        let right = make_table(vec![3, 4, 5], vec![30, 40, 50])?;
        let left_view = left.into_view();
        let right_view = right.into_view();

        let result = crate::execute_cudf(cross_join(&left_view, &right_view))?;

        assert_eq!(result.num_rows(), 6);
        assert_eq!(result.num_columns(), 4);
        Ok(())
    }
}
