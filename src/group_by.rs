use crate::errors::Result;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumnView, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use libcudf_sys::ffi::{
    aggregation_request_create, make_count_aggregation_groupby, make_max_aggregation_groupby,
    make_mean_aggregation_groupby, make_min_aggregation_groupby, make_sum_aggregation_groupby,
    GroupByResult,
};

/// Result of a group-by aggregation operation
///
/// Contains the unique group keys and the aggregation results for each group.
/// Each aggregation request produces one result, which may contain multiple
/// columns depending on the aggregation type.
pub struct CuDFGroupByResult {
    inner: UniquePtr<ffi::GroupByResult>,
}

impl CuDFGroupByResult {
    /// Get a view of the group keys
    ///
    /// The keys table contains the unique combinations of key column values
    /// that define each group.
    pub fn keys(&self) -> CuDFTableView {
        CuDFTableView::new(self.inner.get_keys().view())
    }

    /// Take ownership of the group keys
    ///
    /// This can only be called once. After calling this, methods that access
    /// keys will fail.
    pub fn take_keys(&mut self) -> CuDFTable {
        CuDFTable::from_ptr(self.inner.pin_mut().release_keys())
    }

    /// Get a view of a column from an aggregation result
    ///
    /// # Arguments
    ///
    /// * `index` - The aggregation request index
    /// * `column` - The column index within that aggregation result
    pub fn get_column(&self, index: usize, column: usize) -> CuDFColumnView {
        CuDFColumnView::new(self.inner.get(index).get(column).view())
    }

    /// Take ownership of a column from an aggregation result
    ///
    /// # Arguments
    ///
    /// * `index` - The aggregation request index
    /// * `column` - The column index within that aggregation result
    pub fn take_column(&mut self, index: usize, column: usize) -> CuDFColumnView {
        CuDFColumnView::from_column(
            GroupByResult::get_mut(self.inner.pin_mut(), index).release(column),
        )
    }

    /// Get the number of aggregation results
    pub fn results_len(&self) -> usize {
        self.inner.len()
    }

    /// Get the number of columns in an aggregation result
    ///
    /// # Arguments
    ///
    /// * `index` - The aggregation request index
    pub fn columns_len(&self, index: usize) -> usize {
        self.inner.get(index).len()
    }
}

/// A group-by operation builder
///
/// Groups rows by key columns and computes aggregations on value columns
/// for each group. Created with key columns and then used to perform
/// multiple aggregations.
pub struct CuDFGroupBy {
    inner: UniquePtr<ffi::GroupBy>,
}

impl CuDFGroupBy {
    /// Create a group-by operation with the specified key columns
    ///
    /// The keys determine how rows are grouped. Rows with matching key
    /// values will be grouped together for aggregation.
    pub fn from(view: &CuDFTableView) -> Self {
        Self {
            inner: ffi::groupby_create(view.inner()),
        }
    }

    /// Perform aggregations on the grouped data
    ///
    /// Each request specifies a column to aggregate and the aggregations
    /// to perform on it. Returns the unique group keys and aggregation
    /// results for each group.
    pub fn aggregate(&self, requests: &[AggregationRequest]) -> Result<CuDFGroupByResult> {
        let requests = requests
            .iter()
            .map(|x| x.inner.as_ptr())
            .collect::<Vec<_>>();

        Ok(CuDFGroupByResult {
            inner: self.inner.aggregate(&requests)?,
        })
    }
}

/// A request to aggregate a column in a group-by operation
///
/// Specifies a column of values to aggregate and the aggregations to
/// perform on it. Multiple aggregations can be added to a single request.
pub struct AggregationRequest {
    inner: UniquePtr<ffi::AggregationRequest>,
}

impl AggregationRequest {
    /// Create an aggregation request for a column
    ///
    /// The group membership of each value is determined by the corresponding
    /// row in the keys used to construct the groupby.
    pub fn new(view: &CuDFColumnView) -> Self {
        Self {
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
