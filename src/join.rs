use crate::{CuDFError, CuDFRef, CuDFTable, CuDFTableView};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use std::sync::Arc;

fn select_cols(view: &CuDFTableView, cols: &[usize]) -> cxx::UniquePtr<ffi::TableView> {
    let indices: Vec<i32> = cols.iter().map(|&i| i as i32).collect();
    view.inner().select(&indices)
}

/// Reusable hash join built from a fixed build-side key table.
///
/// The build-side table is kept alive for the lifetime of this object.
pub struct CuDFHashJoin {
    inner: UniquePtr<ffi::HashJoin>,
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
    fn nulls_equal(self) -> bool {
        matches!(self, Self::Equal)
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
        let inner = ffi::hash_join_create(&build_keys, null_equality.nulls_equal())?;
        Ok(Self {
            inner,
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
        let result = ffi::hash_join_inner_join_gather(
            &self.inner,
            &probe_keys,
            selected_build_payload
                .as_ref()
                .unwrap_or_else(|| build_payload.inner()),
            selected_probe_payload
                .as_ref()
                .unwrap_or_else(|| probe_payload.inner()),
        )?;
        Ok(CuDFTable::from_inner(result))
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
    let result = ffi::inner_join_gather(
        &left_keys,
        &right_keys,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
    )?;
    Ok(CuDFTable::from_inner(result))
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
    let result = ffi::left_join_gather(
        &left_keys,
        &right_keys,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
    )?;
    Ok(CuDFTable::from_inner(result))
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
    let result = ffi::full_join_gather(
        &left_keys,
        &right_keys,
        left_payload.as_ref().unwrap_or_else(|| left.inner()),
        right_payload.as_ref().unwrap_or_else(|| right.inner()),
    )?;
    Ok(CuDFTable::from_inner(result))
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
    Ok(CuDFTable::from_inner(ffi::left_semi_join_gather(
        &select_cols(left, left_on),
        &select_cols(right, right_on),
        left.inner(),
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
    Ok(CuDFTable::from_inner(ffi::left_anti_join_gather(
        &select_cols(left, left_on),
        &select_cols(right, right_on),
        left.inner(),
    )?))
}

/// Perform a cross join (Cartesian product) of two tables.
///
/// Returns all combinations of rows from both inputs.
pub fn cross_join(left: &CuDFTableView, right: &CuDFTableView) -> Result<CuDFTable, CuDFError> {
    Ok(CuDFTable::from_inner(ffi::cross_join(
        left.inner(),
        right.inner(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
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
        Ok(())
    }

    #[test]
    fn test_left_anti_join() -> Result<(), Box<dyn std::error::Error>> {
        let left = make_table(vec![1, 2, 3, 4], vec![10, 20, 30, 40]);
        let right = make_table(vec![2, 3, 5], vec![200, 300, 500]);

        let result = left_anti_join(&left.into_view(), &right.into_view(), &[0], &[0])?;
        assert_eq!(result.num_rows(), 2); // keys 1 and 4 have no match
        assert_eq!(result.num_columns(), 2); // only left columns
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
