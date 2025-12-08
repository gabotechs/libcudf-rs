use crate::cudf_reference::CuDFRef;
use crate::errors::Result;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumn, CuDFColumnView, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use libcudf_sys::ffi::{
    aggregation_request_create, make_count_aggregation_groupby, make_max_aggregation_groupby,
    make_mean_aggregation_groupby, make_min_aggregation_groupby, make_sum_aggregation_groupby,
};
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
    pub fn from(view: &CuDFTableView) -> Self {
        Self {
            _ref: view._ref.clone(),
            inner: ffi::groupby_create(view.inner()),
        }
    }

    /// Perform aggregations on the grouped data
    ///
    /// Each request specifies a column to aggregate and the aggregations
    /// to perform on it. Returns the unique group keys and aggregation
    /// results for each group.
    pub fn aggregate(
        &self,
        requests: &[AggregationRequest],
    ) -> Result<(CuDFTable, Vec<Vec<CuDFColumn>>)> {
        let mut _refs = Vec::with_capacity(requests.len());
        let requests = requests
            .iter()
            .map(|x| {
                _refs.push(x._ref.clone());
                x.inner.as_ptr()
            })
            .collect::<Vec<_>>();
        let mut gby_result = self.inner.aggregate(&requests)?;
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
    pub fn new(view: &CuDFColumnView) -> Self {
        Self {
            _ref: view._ref.clone(),
            inner: aggregation_request_create(view.inner()),
        }
    }

    /// Add an aggregation to this request
    ///
    /// Multiple aggregations can be added to aggregate the same column
    /// in different ways (e.g., both SUM and COUNT).
    pub fn add(&mut self, aggregation: Aggregation) {
        self.inner.add(aggregation.inner);
    }
}

/// An aggregation operation specification
///
/// Specifies how to aggregate values within each group. Created using
/// factory methods from `AggregationOp`.
pub struct Aggregation {
    inner: UniquePtr<ffi::Aggregation>,
}

impl Aggregation {
    /// Create an aggregation from a cuDF aggregation pointer
    pub fn new(inner: UniquePtr<ffi::Aggregation>) -> Self {
        Self { inner }
    }
}

/// Types of aggregation operations
///
/// Specifies the aggregation function to apply to grouped values.
pub enum AggregationOp {
    /// Sum of values
    SUM,
    /// Minimum value
    MIN,
    /// Maximum value
    MAX,
    /// Arithmetic mean
    MEAN,
    /// Count of values
    COUNT,
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
    pub fn group_by(&self) -> Aggregation {
        use AggregationOp::*;
        match self {
            SUM => Aggregation::new(make_sum_aggregation_groupby()),
            MIN => Aggregation::new(make_min_aggregation_groupby()),
            MAX => Aggregation::new(make_max_aggregation_groupby()),
            MEAN => Aggregation::new(make_mean_aggregation_groupby()),
            COUNT => Aggregation::new(make_count_aggregation_groupby()),
        }
    }
}
