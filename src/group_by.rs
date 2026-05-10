use crate::cudf_reference::CuDFRef;
use crate::device_resource::resource_ref;
use crate::errors::Result;
use crate::stream::stream_ref;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumn, CuDFColumnView, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::ffi::{
    self, aggregation_request_create, aggregation_requests_create, make_count_aggregation_groupby,
    make_max_aggregation_groupby, make_mean_aggregation_groupby, make_median_aggregation_groupby,
    make_min_aggregation_groupby, make_nunique_aggregation_groupby, make_std_aggregation_groupby,
    make_sum_aggregation_groupby, make_variance_aggregation_groupby,
};
use libcudf_sys::{NullPolicy, Sorted};
use std::sync::Arc;

/// A group-by operation builder
///
/// Groups rows by key columns and computes aggregations on value columns
/// for each group. Created with key columns and then used to perform
/// multiple aggregations.
pub struct CuDFGroupBy {
    _ref: Option<Arc<dyn CuDFRef>>,
    inner: UniquePtr<ffi::GroupBy>,
}

impl CuDFGroupBy {
    /// Create a group-by operation with the specified key columns
    ///
    /// The keys determine how rows are grouped. Rows with matching key
    /// values will be grouped together for aggregation.
    pub fn from_table_view(view: CuDFTableView) -> Self {
        let column_order: &[i32] = &[];
        let null_precedence: &[i32] = &[];
        let inner = ffi::groupby_create(
            view.inner(),
            NullPolicy::Exclude as i32,
            Sorted::No as i32,
            column_order,
            null_precedence,
        );
        Self {
            _ref: Some(Arc::new(view)),
            inner,
        }
    }

    /// Perform aggregations on the grouped data
    ///
    /// Each request specifies a column to aggregate and the aggregations
    /// to perform on it. Returns the unique group keys and aggregation
    /// results for each group.
    pub fn aggregate<I>(&self, requests: I) -> Result<(CuDFTable, Vec<Vec<CuDFColumn>>)>
    where
        I: IntoIterator<Item = AggregationRequest>,
    {
        let mut _refs = Vec::new();
        let mut requests_inner = aggregation_requests_create();
        for request in requests {
            _refs.push(request._ref.clone());
            requests_inner.pin_mut().add(request.inner);
        }

        let stream = ffi::get_default_stream();
        let mr = ffi::get_current_device_resource_ref();
        let mut gby_result =
            self.inner
                .aggregate(&requests_inner, stream_ref(&stream)?, resource_ref(&mr)?)?;
        let keys = gby_result.pin_mut().release_keys();
        let keys = CuDFTable::from_ptr(keys);

        let mut results = Vec::with_capacity(gby_result.len());
        for i in 0..gby_result.len() {
            let mut released_result = gby_result.pin_mut().release_result(i);
            let mut cols = Vec::with_capacity(released_result.len());
            for j in 0..released_result.len() {
                let col = released_result.pin_mut().release(j);
                cols.push(CuDFColumn::new(col));
            }

            results.push(cols)
        }
        Ok((keys, results))
    }
}

/// A request to aggregate a column in a group-by operation
///
/// Specifies a column of values to aggregate and the aggregations to
/// perform on it. Multiple aggregations can be added to a single request.
pub struct AggregationRequest {
    _ref: Option<Arc<dyn CuDFRef>>,
    inner: UniquePtr<ffi::AggregationRequest>,
}

impl AggregationRequest {
    /// Create an aggregation request for a column
    ///
    /// The group membership of each value is determined by the corresponding
    /// row in the keys used to construct the groupby.
    pub fn from_column_view(view: CuDFColumnView) -> Self {
        let inner = aggregation_request_create(view.inner());
        Self {
            _ref: Some(Arc::new(view)),
            inner,
        }
    }

    /// Add an aggregation to this request
    ///
    /// Multiple aggregations can be added to aggregate the same column
    /// in different ways (e.g., both SUM and COUNT).
    pub fn add(&mut self, aggregation: GroupByAggregation) {
        self.inner.add(aggregation.inner);
    }
}

/// A groupby aggregation operation specification
///
/// Specifies how to aggregate values within each group. Created using
/// factory methods from `AggregationOp`.
pub struct GroupByAggregation {
    inner: UniquePtr<ffi::GroupByAggregation>,
}

impl GroupByAggregation {
    /// Create a groupby aggregation from a cuDF aggregation pointer
    pub fn new(inner: UniquePtr<ffi::GroupByAggregation>) -> Self {
        Self { inner }
    }
}

/// Backwards-compatible alias for groupby aggregations.
pub type Aggregation = GroupByAggregation;

/// Types of aggregation operations
///
/// Specifies the aggregation function to apply to grouped values.
#[allow(non_camel_case_types)]
pub enum AggregationOp {
    /// Sum of values
    SUM,
    /// Minimum value
    MIN,
    /// Maximum value
    MAX,
    /// Arithmetic mean
    MEAN,
    /// Count of non-null values
    COUNT,
    /// Count of all values, including nulls
    COUNT_ALL,
    /// Variance with delta degrees of freedom
    ///
    /// - `ddof = 0`: Population std dev
    /// - `ddof = 1`: Sample std dev
    VARIANCE { ddof: i32 },
    /// Standard deviation with delta degrees of freedom
    ///
    /// - `ddof = 0`: Population std dev
    /// - `ddof = 1`: Sample std dev
    STD { ddof: i32 },
    /// Count of distinct, non-null values
    NUNIQUE,
    /// Median value
    MEDIAN,
}

impl AggregationOp {
    /// Create an aggregation for use in group-by operations
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use libcudf_rs::AggregationOp;
    ///
    /// let sum_agg = AggregationOp::SUM.group_by();
    /// let count_agg = AggregationOp::COUNT.group_by();
    /// ```
    pub fn group_by(&self) -> GroupByAggregation {
        use AggregationOp::*;
        match self {
            SUM => GroupByAggregation::new(make_sum_aggregation_groupby()),
            MIN => GroupByAggregation::new(make_min_aggregation_groupby()),
            MAX => GroupByAggregation::new(make_max_aggregation_groupby()),
            MEAN => GroupByAggregation::new(make_mean_aggregation_groupby()),
            COUNT => {
                GroupByAggregation::new(make_count_aggregation_groupby(NullPolicy::Exclude as i32))
            }
            COUNT_ALL => {
                GroupByAggregation::new(make_count_aggregation_groupby(NullPolicy::Include as i32))
            }
            VARIANCE { ddof } => GroupByAggregation::new(make_variance_aggregation_groupby(*ddof)),
            STD { ddof } => GroupByAggregation::new(make_std_aggregation_groupby(*ddof)),
            NUNIQUE => GroupByAggregation::new(make_nunique_aggregation_groupby(
                NullPolicy::Exclude as i32,
            )),
            MEDIAN => GroupByAggregation::new(make_median_aggregation_groupby()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{make_array, Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_sum_integers() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // SUM([1, 2, 3, 4, 5]) = 15
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let result = run_single_group_agg::<Int64Array>(&values, AggregationOp::SUM)?;
        assert_eq!(result.value(0), 15);
        Ok(())
    }

    #[test]
    fn test_sum_with_nulls() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // SUM([1, NULL, 3, NULL, 5]) = 9
        let values = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let result = run_single_group_agg::<Int64Array>(&values, AggregationOp::SUM)?;
        assert_eq!(result.value(0), 9);
        Ok(())
    }

    #[test]
    fn test_min() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // MIN([5, 2, 8, 1, 9]) = 1
        let values = Int32Array::from(vec![5, 2, 8, 1, 9]);
        let result = run_single_group_agg::<Int32Array>(&values, AggregationOp::MIN)?;
        assert_eq!(result.value(0), 1);
        Ok(())
    }

    #[test]
    fn test_max() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // MAX([5, 2, 8, 1, 9]) = 9
        let values = Int32Array::from(vec![5, 2, 8, 1, 9]);
        let result = run_single_group_agg::<Int32Array>(&values, AggregationOp::MAX)?;
        assert_eq!(result.value(0), 9);
        Ok(())
    }

    #[test]
    fn test_mean() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // MEAN([10, 20, 30, 40, 50]) = 30
        let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let result = run_single_group_agg::<Float64Array>(&values, AggregationOp::MEAN)?;
        assert!((result.value(0) - 30.0).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn test_count() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // COUNT([1, 2, 3, 4, 5]) = 5
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let result = run_single_group_agg::<Int32Array>(&values, AggregationOp::COUNT)?;
        assert_eq!(result.value(0), 5);
        Ok(())
    }

    #[test]
    fn test_count_excludes_nulls() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // COUNT([1, NULL, 3, NULL, 5]) = 3
        let values = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        let result = run_single_group_agg::<Int32Array>(&values, AggregationOp::COUNT)?;
        assert_eq!(result.value(0), 3);
        Ok(())
    }

    #[test]
    fn test_variance_sample() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // VARIANCE([1, 2, 3, 4, 5]) with ddof=1 = 2.5
        let values = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result =
            run_single_group_agg::<Float64Array>(&values, AggregationOp::VARIANCE { ddof: 1 })?;
        assert!((result.value(0) - 2.5).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn test_std_sample() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // STD([1, 2, 3, 4, 5]) with ddof=1 = sqrt(2.5)
        let values = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = run_single_group_agg::<Float64Array>(&values, AggregationOp::STD { ddof: 1 })?;
        let expected = (2.5_f64).sqrt();
        assert!((result.value(0) - expected).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn test_nunique() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // NUNIQUE([1, 2, 2, 3, 3, 3]) = 3
        let values = Int32Array::from(vec![1, 2, 2, 3, 3, 3]);
        let result = run_single_group_agg::<Int32Array>(&values, AggregationOp::NUNIQUE)?;
        assert_eq!(result.value(0), 3);
        Ok(())
    }

    #[test]
    fn test_median() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // MEDIAN([1, 2, 3, 4, 5]) = 3
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let result = run_single_group_agg::<Float64Array>(&values, AggregationOp::MEDIAN)?;
        assert!((result.value(0) - 3.0).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn test_multiple_groups() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // Group 1: [1, 2, 3] sums to 6.
        // Group 2: [10, 20] sums to 30.
        let values = Int32Array::from(vec![1, 2, 3, 10, 20]);
        let keys = Int32Array::from(vec![1, 1, 1, 2, 2]);

        let values_col = CuDFColumn::from_arrow_host(&values)?;
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let keys_batch = RecordBatch::try_new(schema, vec![Arc::new(keys)])?;
        let keys_table = CuDFTable::from_arrow_host(keys_batch)?;
        let groupby = CuDFGroupBy::from_table_view(keys_table.into_view());

        let mut request = AggregationRequest::from_column_view(values_col.into_view());
        request.add(AggregationOp::SUM.group_by());

        let (result_keys, results) = groupby.aggregate([request])?;

        assert_eq!(result_keys.num_rows(), 2);

        let sum_col = results
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let sum_array = sum_col.into_view().to_arrow_host()?;
        let sum_values = sum_array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Should have sums [6, 30] in some order
        let sum1 = sum_values.value(0);
        let sum2 = sum_values.value(1);
        assert!(
            (sum1 == 6 && sum2 == 30) || (sum1 == 30 && sum2 == 6),
            "Expected sums [6, 30], got [{}, {}]",
            sum1,
            sum2
        );

        Ok(())
    }

    fn make_keys_table(keys: &dyn Array) -> Result<CuDFTable> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            keys.data_type().clone(),
            false,
        )]));
        let keys_data = keys.to_data();
        let batch = RecordBatch::try_new(schema, vec![make_array(keys_data)])?;
        CuDFTable::from_arrow_host(batch)
    }

    fn run_single_group_agg<T: Array + Clone + 'static>(
        values: &dyn Array,
        agg_op: AggregationOp,
    ) -> Result<Arc<T>> {
        let n = values.len();
        let keys = Int32Array::from(vec![1; n]);

        let values_col = CuDFColumn::from_arrow_host(values)?;
        let keys_table = make_keys_table(&keys)?;
        let groupby = CuDFGroupBy::from_table_view(keys_table.into_view());

        let mut request = AggregationRequest::from_column_view(values_col.into_view());
        request.add(agg_op.group_by());

        let (_result_keys, results) = groupby.aggregate([request])?;
        let result_col = results
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let result_array = result_col.into_view().to_arrow_host()?;

        Ok(Arc::new(
            (*result_array.as_any().downcast_ref::<T>().unwrap()).clone(),
        ))
    }
}
