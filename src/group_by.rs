use crate::errors::Result;
use crate::table_view::CuDFTableView;
use crate::{CuDFColumnView, CuDFTable};
use cxx::UniquePtr;
use libcudf_sys::ffi;
use libcudf_sys::ffi::{
    aggregation_request_create, make_count_aggregation_groupby, make_max_aggregation_groupby,
    make_mean_aggregation_groupby, make_min_aggregation_groupby, make_sum_aggregation_groupby,
};

/// A GPU-accelerated table (similar to a DataFrame)
///
/// This is a safe wrapper around cuDF's group-by result type.
pub struct CuDFGroupByResult {
    inner: UniquePtr<ffi::GroupByResult>,
}

impl CuDFGroupByResult {
    pub fn keys(&self) -> CuDFTableView {
        CuDFTableView::new(self.inner.get_keys().view())
    }

    pub fn column(&self, index: usize, column: usize) -> CuDFColumnView {
        CuDFColumnView::new(self.inner.get(index).get(column).view())
    }

    pub fn results_len(&self) -> usize {
        self.inner.len()
    }

    pub fn columns_len(&self, index: usize) -> usize {
        self.inner.get(index).len()
    }
}

pub struct CuDFGroupBy {
    inner: UniquePtr<ffi::GroupBy>,
}

impl CuDFGroupBy {
    pub fn from(view: &CuDFTableView) -> Self {
        Self {
            inner: ffi::groupby_create(view.inner()),
        }
    }

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

pub struct AggregationRequest {
    inner: UniquePtr<ffi::AggregationRequest>,
}

impl AggregationRequest {
    pub fn new(view: &CuDFColumnView) -> Self {
        Self {
            inner: aggregation_request_create(view.inner()),
        }
    }

    pub fn add(&mut self, aggregation: Aggregation) {
        self.inner.add(aggregation.inner);
    }
}

pub struct Aggregation {
    inner: UniquePtr<ffi::Aggregation>,
}

impl Aggregation {
    pub fn new(inner: UniquePtr<ffi::Aggregation>) -> Self {
        Self { inner }
    }
}

pub enum AggregationOp {
    SUM,
    MIN,
    MAX,
    MEAN,
    COUNT,
}

impl AggregationOp {
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
