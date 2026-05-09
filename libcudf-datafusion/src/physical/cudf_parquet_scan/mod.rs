mod read_plan;
mod reader;
mod scan_source;

use self::read_plan::{FileBatch, ReadPlan};
use self::reader::ParquetBatchReader;
pub(crate) use self::scan_source::{
    CuDFParquetSource, CuDFParquetSourceBuilder, CuDFParquetSourceError, RowGroupSelection,
};
use crate::errors::cudf_to_df;
use crate::metrics::CuDFBaselineMetrics;
use crate::planner::DEFAULT_PARQUET_SCAN_FILES_PER_BATCH;
use arrow::array::{Array, ArrayRef};
use arrow_schema::SchemaRef;
use datafusion::common::plan_err;
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion_physical_plan::stream::RecordBatchReceiverStream;
use datafusion_physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use libcudf_rs::{cast, synchronize_default_stream, CuDFAstExpression};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// Configuration for a cuDF-backed Parquet scan.
#[derive(Debug, Clone)]
pub struct CuDFParquetScanConfig {
    file_groups: Vec<Vec<CuDFParquetSource>>,
    file_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filter: Option<Arc<CuDFAstExpression>>,
    files_per_batch: usize,
}

impl CuDFParquetScanConfig {
    pub(crate) fn from_source_groups(
        file_groups: Vec<Vec<CuDFParquetSource>>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            file_groups,
            file_schema,
            projection: None,
            filter: None,
            files_per_batch: DEFAULT_PARQUET_SCAN_FILES_PER_BATCH,
        }
    }

    /// Set an optional projection using file schema column indices.
    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Set an optional cuDF AST filter.
    pub fn with_filter(mut self, filter: Option<Arc<CuDFAstExpression>>) -> Self {
        self.filter = filter;
        self
    }

    /// Set the maximum number of files per cuDF read.
    pub fn with_files_per_batch(mut self, files_per_batch: usize) -> Self {
        self.files_per_batch = files_per_batch;
        self
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<CuDFParquetSource>>) -> Self {
        self.file_groups = file_groups;
        self
    }
}

/// DataFusion execution plan that scans Parquet files directly with cuDF.
#[derive(Debug)]
pub struct CuDFParquetScanExec {
    config: CuDFParquetScanConfig,
    read_plan: ReadPlan,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CuDFParquetScanExec {
    /// Create a cuDF Parquet scan from a validated scan config.
    pub fn try_new(config: CuDFParquetScanConfig) -> datafusion::common::Result<Self> {
        let file_count = ReadPlan::source_count(&config.file_groups);
        if file_count == 0 {
            return plan_err!("CuDFParquetScanExec requires at least one parquet file");
        }
        if config.files_per_batch == 0 {
            return plan_err!("CuDFParquetScanExec files_per_batch must be greater than zero");
        }

        let output_schema = project_schema(&config.file_schema, config.projection.as_ref())?;
        let read_columns = parquet_read_columns(&config)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(super::cudf_schema_compatibility_map(output_schema)),
            Partitioning::UnknownPartitioning(config.file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let read_plan = ReadPlan::new(
            config.file_groups.clone(),
            read_columns,
            config.files_per_batch,
        );

        Ok(Self {
            config,
            read_plan,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

fn parquet_read_columns(
    config: &CuDFParquetScanConfig,
) -> datafusion::common::Result<Option<Arc<[String]>>> {
    let Some(projection) = config.projection.as_ref() else {
        return Ok(None);
    };
    if projection.is_empty() && config.filter.is_some() {
        let Some(field) = config.file_schema.fields().first() else {
            return Ok(Some(Arc::from(Vec::<String>::new())));
        };
        return Ok(Some(Arc::from(vec![field.name().clone()])));
    }

    let mut columns = Vec::with_capacity(projection.len());
    for &index in projection {
        let Some(field) = config.file_schema.fields().get(index) else {
            return plan_err!(
                "CuDFParquetScanExec projection index {index} out of bounds for schema with {} fields",
                config.file_schema.fields().len()
            );
        };
        columns.push(field.name().clone());
    }
    Ok(Some(Arc::from(columns)))
}

impl DisplayAs for CuDFParquetScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let read_columns = self
            .read_plan
            .read_column_count()
            .map_or_else(|| "all".to_string(), |count| count.to_string());
        write!(
            f,
            "CuDFParquetScanExec: files={}, batches={}, files_per_batch={}, read_columns={}, filter={}",
            self.read_plan.file_count(),
            self.read_plan.batch_count(),
            self.read_plan.files_per_batch(),
            read_columns,
            self.config.filter.is_some()
        )
    }
}

impl ExecutionPlan for CuDFParquetScanExec {
    fn name(&self) -> &str {
        "CuDFParquetScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!(
                "CuDFParquetScanExec expects no children, {} were provided",
                children.len()
            );
        }
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(file_groups) =
            ReadPlan::repartitioned_source_groups(&self.config.file_groups, target_partitions)
        else {
            return Ok(None);
        };

        let config = self.config.clone().with_file_groups(file_groups);
        Ok(Some(Arc::new(Self::try_new(config)?)))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let Some(scan_partition) = self.read_plan.partition(partition) else {
            return plan_err!("CuDFParquetScanExec invalid partition {partition}");
        };

        let schema = self.schema();
        let reader = ParquetBatchReader::new(
            Arc::clone(&schema),
            self.read_plan.read_columns(),
            self.config.filter.clone(),
        );
        let metrics = ScanMetrics::new(&self.metrics, partition);
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
        let output = builder.tx();

        builder.spawn_blocking(move || {
            for file_batch in scan_partition.batches() {
                let compute_timer = metrics.baseline.elapsed_compute().timer();
                let result: datafusion::common::Result<_> = (|| {
                    metrics.record_file_batch(file_batch);

                    let read_timer = metrics.read_time.timer();
                    let parquet_batch = reader.read(file_batch)?;
                    read_timer.done();

                    let batch = build_record_batch(parquet_batch, &schema, &metrics)?;
                    metrics.baseline.record_output(&batch);
                    Ok(batch)
                })();
                compute_timer.done();
                let batch = result?;

                let send_timer = metrics.output_send_time.timer();
                let send_result = output.blocking_send(Ok(batch));
                send_timer.done();
                if send_result.is_err() {
                    break;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn build_record_batch(
    parquet_batch: reader::ReadBatch,
    schema: &SchemaRef,
    metrics: &ScanMetrics,
) -> datafusion::common::Result<arrow::record_batch::RecordBatch> {
    let cast_timer = metrics.cast_time.timer();
    let mut cudf_cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (column, field) in parquet_batch.columns.into_iter().zip(schema.fields()) {
        let view = column.into_view();
        let column: ArrayRef = if view.data_type() == field.data_type() {
            Arc::new(view)
        } else {
            let casted = cast(&view, field.data_type()).map_err(cudf_to_df)?;
            Arc::new(casted.into_view())
        };
        cudf_cols.push(column);
    }
    cast_timer.done();

    let sync_timer = metrics.sync_time.timer();
    synchronize_default_stream().map_err(cudf_to_df)?;
    sync_timer.done();

    let output_batch_timer = metrics.output_batch_time.timer();
    let batch = libcudf_rs::record_batch_with_schema(cudf_cols, schema, parquet_batch.num_rows)?;
    output_batch_timer.done();

    Ok(batch)
}

/// Metrics for one executing Parquet scan partition.
#[derive(Clone)]
struct ScanMetrics {
    baseline: CuDFBaselineMetrics,
    read_time: Time,
    cast_time: Time,
    sync_time: Time,
    output_batch_time: Time,
    output_send_time: Time,
    files: Count,
    file_bytes: Count,
}

impl ScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: CuDFBaselineMetrics::new(metrics, partition),
            read_time: MetricBuilder::new(metrics).subset_time("read_time", partition),
            cast_time: MetricBuilder::new(metrics).subset_time("cast_time", partition),
            sync_time: MetricBuilder::new(metrics).subset_time("sync_time", partition),
            output_batch_time: MetricBuilder::new(metrics)
                .subset_time("output_batch_time", partition),
            output_send_time: MetricBuilder::new(metrics)
                .subset_time("output_send_time", partition),
            files: MetricBuilder::new(metrics).counter("files", partition),
            file_bytes: MetricBuilder::new(metrics).counter("file_bytes", partition),
        }
    }

    fn record_file_batch(&self, batch: &FileBatch) {
        self.files.add(batch.sources().len());
        self.file_bytes.add(batch.file_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::{CuDFParquetScanConfig, CuDFParquetScanExec, CuDFParquetSource, RowGroupSelection};
    use arrow::array::{Array, ArrayRef, Int32Array, Scalar};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion_physical_plan::{execute_stream, ExecutionPlan};
    use futures_util::TryStreamExt;
    use libcudf_rs::{CuDFAstExpression, CuDFAstOperator, CuDFColumnView, CuDFScalar};
    use parquet::arrow::ArrowWriter;
    use std::fs::{remove_file, File};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn filters_with_column_outside_projection() -> Result<(), Box<dyn std::error::Error>> {
        let path = temp_parquet_file();
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )?;
        write_parquet(&path, &batch)?;

        let filter = Arc::new(greater_than_filter("id", 2)?);
        let projected = execute_scan(
            scan_config(path.clone(), Arc::clone(&schema))
                .with_projection(Some(vec![0]))
                .with_filter(Some(Arc::clone(&filter))),
        )
        .await?;
        let empty_projection = execute_scan(
            scan_config(path.clone(), schema)
                .with_projection(Some(vec![]))
                .with_filter(Some(filter)),
        )
        .await?;

        remove_file(path).ok();
        assert_eq!(projected.len(), 1);
        assert_eq!(
            cudf_i32_values(projected[0].column(0))?,
            vec![Some(30), Some(40)]
        );
        assert_eq!(empty_projection.len(), 1);
        assert_eq!(empty_projection[0].num_columns(), 0);
        assert_eq!(empty_projection[0].num_rows(), 2);

        Ok(())
    }

    async fn execute_scan(
        config: CuDFParquetScanConfig,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CuDFParquetScanExec::try_new(config)?);
        Ok(execute_stream(plan, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?)
    }

    fn scan_config(path: PathBuf, schema: Arc<Schema>) -> CuDFParquetScanConfig {
        CuDFParquetScanConfig::from_source_groups(
            vec![vec![CuDFParquetSource {
                path,
                byte_len: 0,
                row_groups: RowGroupSelection::All,
            }]],
            schema,
        )
    }

    fn write_parquet(
        path: &PathBuf,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    fn greater_than_filter(
        column: &str,
        value: i32,
    ) -> Result<CuDFAstExpression, Box<dyn std::error::Error>> {
        let mut filter = CuDFAstExpression::new();
        let column = filter.column_name_reference(column)?;
        let value = CuDFScalar::from_arrow_host(Scalar::new(&Int32Array::from(vec![value])))?;
        let value = filter.literal(value)?;
        filter.binary_operation(CuDFAstOperator::Greater, column, value)?;
        Ok(filter)
    }

    fn cudf_i32_values(column: &ArrayRef) -> Result<Vec<Option<i32>>, Box<dyn std::error::Error>> {
        let column = column
            .as_any()
            .downcast_ref::<CuDFColumnView>()
            .expect("parquet scan should return cuDF columns");
        let host = column.to_arrow_host()?;
        let ints = host
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column should be Int32");
        Ok((0..ints.len())
            .map(|index| {
                if ints.is_null(index) {
                    None
                } else {
                    Some(ints.value(index))
                }
            })
            .collect())
    }

    fn temp_parquet_file() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "libcudf-datafusion-scan-{}-{nanos}.parquet",
            std::process::id()
        ))
    }
}
