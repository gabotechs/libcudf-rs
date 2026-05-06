//! GPU-safe analogue of [`datafusion::physical_plan::metrics::BaselineMetrics`].
//!
//! Upstream's `BaselineMetrics::record_poll` -> `RecordOutput::record_output` for
//! `RecordBatch` calls [`get_record_batch_memory_size`], which iterates each
//! column with [`Array::to_data`]. For GPU-backed columns ([`CuDFColumnView`]),
//! `to_data` triggers a full GPU -> host copy (and a debug-only `panic!`),
//! which we cannot afford in any operator that emits cuDF batches.
//!
//! [`CuDFBaselineMetrics`] mirrors upstream's surface (same metric names so
//! `EXPLAIN ANALYZE` looks identical) and sizes batches via
//! [`Array::get_array_memory_size`] instead — that method is implemented
//! directly by `CuDFColumnView` in terms of the underlying device buffer
//! sizes and never copies data to the host.
//!
//! [`get_record_batch_memory_size`]: datafusion::common::utils::memory::get_record_batch_memory_size
//! [`Array::to_data`]: Array::to_data
//! [`CuDFColumnView`]: libcudf_rs::CuDFColumnView
//! [`Array::get_array_memory_size`]: Array::get_array_memory_size

use arrow::array::{Array, RecordBatch};
use datafusion::error::DataFusionError;
use datafusion::physical_expr_common::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, Time, Timestamp,
};
use std::task::Poll;

/// GPU-safe replacement for [`datafusion::physical_plan::metrics::BaselineMetrics`].
///
/// Records the same metrics under the same names — `start_timestamp`,
/// `end_timestamp`, `elapsed_compute`, `output_rows`, `output_bytes`,
/// `output_batches` — so plans show up unchanged in `EXPLAIN ANALYZE`.
#[derive(Debug, Clone)]
pub(crate) struct CuDFBaselineMetrics {
    /// Recorded when this struct is dropped (or `done()` is called).
    end_time: Timestamp,
    /// CPU-side compute time. Wrap hot sections in `elapsed_compute().timer()`.
    elapsed_compute: Time,
    /// Total output rows.
    output_rows: Count,
    /// Total output-batch bytes, sized via [`Array::get_array_memory_size`]
    /// (GPU-safe — never calls `to_data`).
    ///
    /// May overestimate slightly when columns share buffers, since this does
    /// not dedupe by buffer pointer like upstream's host-side traversal does.
    output_bytes: Count,
    /// Total output batches.
    output_batches: Count,
}

impl CuDFBaselineMetrics {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .end_timestamp(partition),
            elapsed_compute: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .elapsed_compute(partition),
            output_rows: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .output_rows(partition),
            output_bytes: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .output_bytes(partition),
            output_batches: MetricBuilder::new(metrics)
                .with_type(MetricType::DEV)
                .output_batches(partition),
        }
    }

    /// Compute-time clock. Wrap hot sections in `metrics.elapsed_compute().timer()`.
    pub(crate) fn elapsed_compute(&self) -> &Time {
        &self.elapsed_compute
    }

    /// Records that `batch` was emitted, including row count, batch count,
    /// and per-column memory size. Safe to call on GPU-backed batches.
    pub(crate) fn record_output(&self, batch: &RecordBatch) {
        self.output_rows.add(batch.num_rows());
        self.output_batches.add(1);
        let bytes: usize = batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum();
        self.output_bytes.add(bytes);
    }

    /// Process a poll result of a stream producing output for an operator.
    ///
    /// Mirrors [`BaselineMetrics::record_poll`] but never reaches into
    /// column data, so it is safe for streams that emit GPU-backed batches.
    /// Only `output_rows`, `output_bytes`, `output_batches`, and `end_time`
    /// are touched here; `elapsed_compute` must be updated manually.
    ///
    /// [`BaselineMetrics::record_poll`]: datafusion::physical_plan::metrics::BaselineMetrics::record_poll
    pub(crate) fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch, DataFusionError>>>,
    ) -> Poll<Option<Result<RecordBatch, DataFusionError>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => self.record_output(batch),
                Some(Err(_)) | None => self.done(),
            }
        }
        poll
    }

    /// Record that the operator's execution is complete.
    pub(crate) fn done(&self) {
        self.end_time.record()
    }

    /// Record `done` if it hasn't been recorded already.
    pub(crate) fn try_done(&self) {
        if self.end_time.value().is_none() {
            self.end_time.record()
        }
    }
}

impl Drop for CuDFBaselineMetrics {
    fn drop(&mut self) {
        self.try_done()
    }
}
