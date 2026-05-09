use crate::expr::ast::parquet_filter_to_cudf_filter;
use crate::physical::{
    CuDFCoalescePartitionsExec, CuDFParquetScanConfig, CuDFParquetScanExec, CuDFParquetSource,
    CuDFParquetSourceBuilder, CuDFParquetSourceError,
};
use crate::planner::CuDFConfig;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::ExecutionPlan;
use libcudf_rs::CuDFAstExpression;
use std::collections::HashSet;
use std::sync::Arc;

struct ParquetScanCandidate<'a> {
    file_scan: &'a FileScanConfig,
    source: &'a ParquetSource,
    coalesce_fetch: Option<Option<usize>>,
}

impl<'a> ParquetScanCandidate<'a> {
    fn try_from_plan(plan: &'a Arc<dyn ExecutionPlan>) -> Result<Option<Self>> {
        let (plan, coalesce_fetch) = match plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
            Some(coalesce) => (coalesce.input(), Some(coalesce.fetch())),
            None => (plan, None),
        };

        let Some(data_source) = plan.as_any().downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };

        let Some((file_scan, source)) = data_source.downcast_to_file_source::<ParquetSource>()
        else {
            return Ok(None);
        };

        Ok(Some(Self {
            file_scan,
            source,
            coalesce_fetch,
        }))
    }

    fn try_scan_config(
        &self,
        cudf_config: &CuDFConfig,
    ) -> std::result::Result<CuDFParquetScanConfig, UnsupportedReason> {
        self.validate_scan_options()?;

        let table_schema = self.source.table_schema();
        let file_groups = self.local_file_groups()?;
        let filter = self.cudf_filter(&file_groups)?;
        let scan_config = CuDFParquetScanConfig::from_source_groups(
            file_groups,
            Arc::clone(table_schema.file_schema()),
        );

        Ok(scan_config
            .with_projection(self.projected_file_columns()?)
            .with_filter(filter)
            .with_files_per_batch(cudf_config.parquet_scan_files_per_batch))
    }

    fn wrap_scan(&self, scan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        match self.coalesce_fetch {
            Some(fetch) => Arc::new(CuDFCoalescePartitionsExec::new(scan).with_fetch(fetch)),
            None => scan,
        }
    }

    fn validate_scan_options(&self) -> std::result::Result<(), UnsupportedReason> {
        use UnsupportedReason::*;

        let table_schema = self.source.table_schema();

        for (unsupported, reason) in [
            (
                !table_schema.table_partition_cols().is_empty(),
                PartitionColumns,
            ),
            (self.file_scan.limit.is_some(), Limit),
            (self.file_scan.preserve_order, PreserveOrder),
            (!self.file_scan.output_ordering.is_empty(), OutputOrdering),
            (
                !supported_parquet_options(self.source.table_parquet_options()),
                ParquetOptions,
            ),
            (self.file_scan.expr_adapter_factory.is_some(), ExprAdapter),
            (
                self.file_scan.file_compression_type.is_compressed(),
                CompressedFile,
            ),
        ] {
            if unsupported {
                return Err(reason);
            }
        }

        Ok(())
    }

    fn cudf_filter(
        &self,
        file_groups: &[Vec<CuDFParquetSource>],
    ) -> std::result::Result<Option<Arc<CuDFAstExpression>>, UnsupportedReason> {
        if !self.source.table_parquet_options().global.pushdown_filters {
            return Ok(None);
        }

        let Some(predicate) = self.source.filter() else {
            return Ok(None);
        };
        if is_noop_scan_predicate(&predicate) {
            return Ok(None);
        }

        let file_schema = self.source.table_schema().file_schema();
        let filter = parquet_filter_to_cudf_filter(predicate.as_ref(), file_schema)
            .map_err(|_| UnsupportedReason::Predicate)?;
        validate_filter_columns(file_groups, &filter.column_names)?;
        Ok(Some(Arc::new(filter.expression)))
    }

    fn local_file_groups(
        &self,
    ) -> std::result::Result<Vec<Vec<CuDFParquetSource>>, UnsupportedReason> {
        if self.file_scan.object_store_url.as_str() != "file:///" {
            return Err(UnsupportedReason::NonLocalObjectStore);
        }

        let mut file_groups = Vec::with_capacity(self.file_scan.file_groups.len());
        let mut file_count = 0usize;
        let mut source_builder = CuDFParquetSourceBuilder::default();
        for group in &self.file_scan.file_groups {
            let mut files = Vec::with_capacity(group.len());
            for file in group.iter() {
                files.push(
                    source_builder
                        .try_from_partitioned_file(file)
                        .map_err(UnsupportedReason::from)?,
                );
                file_count += 1;
            }
            file_groups.push(files);
        }

        if file_count == 0 {
            Err(UnsupportedReason::EmptyFiles)
        } else {
            Ok(file_groups)
        }
    }

    fn projected_file_columns(&self) -> std::result::Result<Option<Vec<usize>>, UnsupportedReason> {
        let Some(projection) = self.source.projection() else {
            return Ok(None);
        };

        let file_schema = self.source.table_schema().file_schema();
        let mut indices = Vec::new();
        for expr in projection.as_ref() {
            let Some(column) = expr.expr.as_any().downcast_ref::<Column>() else {
                return Err(UnsupportedReason::Projection);
            };
            let Some(field) = file_schema.fields().get(column.index()) else {
                return Err(UnsupportedReason::Projection);
            };
            if expr.alias != field.name().as_str() || column.name() != field.name() {
                return Err(UnsupportedReason::Projection);
            }
            indices.push(column.index());
        }

        let is_full_file_projection = indices.len() == file_schema.fields().len()
            && indices.iter().copied().eq(0..file_schema.fields().len());

        if is_full_file_projection {
            Ok(None)
        } else {
            Ok(Some(indices))
        }
    }
}

fn supported_parquet_options(options: &TableParquetOptions) -> bool {
    let default = TableParquetOptions::default();
    let mut normalized = options.clone();
    normalized.global.pushdown_filters = default.global.pushdown_filters;
    normalized.global.binary_as_string = default.global.binary_as_string;
    normalized == default
}

fn validate_filter_columns(
    file_groups: &[Vec<CuDFParquetSource>],
    column_names: &[String],
) -> std::result::Result<(), UnsupportedReason> {
    if column_names.is_empty() {
        return Ok(());
    }

    for source in file_groups.iter().flatten() {
        if source.row_group_selection().is_empty() {
            continue;
        }
        let metadata = source
            .metadata()
            .map_err(|_| UnsupportedReason::FileMetadata)?;
        let available_columns = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.path().string())
            .collect::<HashSet<_>>();
        if column_names
            .iter()
            .any(|column| !available_columns.contains(column))
        {
            return Err(UnsupportedReason::Predicate);
        }
    }

    Ok(())
}

fn is_noop_scan_predicate(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if is_true_literal(expr) {
        return true;
    }
    if let Some(dynamic) = expr.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
        return dynamic
            .current()
            .is_ok_and(|current| is_true_literal(&current));
    }
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return matches!(binary.op(), Operator::And)
            && is_noop_scan_predicate(binary.left())
            && is_noop_scan_predicate(binary.right());
    }
    false
}

fn is_true_literal(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<Literal>()
        .is_some_and(|literal| matches!(literal.value(), ScalarValue::Boolean(Some(true))))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnsupportedReason {
    PartitionColumns,
    Limit,
    PreserveOrder,
    OutputOrdering,
    ParquetOptions,
    ExprAdapter,
    CompressedFile,
    NonLocalObjectStore,
    PartitionValues,
    FileExtensions,
    FileSizeOverflow,
    FileRangeOverflow,
    FileMetadata,
    RowGroupIndexOverflow,
    EmptyFiles,
    Projection,
    Predicate,
}

impl From<CuDFParquetSourceError> for UnsupportedReason {
    fn from(error: CuDFParquetSourceError) -> Self {
        match error {
            CuDFParquetSourceError::PartitionValues => Self::PartitionValues,
            CuDFParquetSourceError::FileExtensions => Self::FileExtensions,
            CuDFParquetSourceError::FileSizeOverflow => Self::FileSizeOverflow,
            CuDFParquetSourceError::FileMetadata => Self::FileMetadata,
            CuDFParquetSourceError::RowGroupIndexOverflow => Self::RowGroupIndexOverflow,
            CuDFParquetSourceError::FileRangeOverflow => Self::FileRangeOverflow,
        }
    }
}

pub(crate) fn try_as_cudf_parquet_scan(
    plan: &Arc<dyn ExecutionPlan>,
    cudf_config: &CuDFConfig,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !cudf_config.parquet_scan {
        return Ok(None);
    }

    let Some(candidate) = ParquetScanCandidate::try_from_plan(plan)? else {
        return Ok(None);
    };

    let scan_config = match candidate.try_scan_config(cudf_config) {
        Ok(scan_config) => scan_config,
        Err(_) => return Ok(None),
    };

    let scan = Arc::new(CuDFParquetScanExec::try_new(scan_config)?) as Arc<dyn ExecutionPlan>;
    Ok(Some(candidate.wrap_scan(scan)))
}

#[cfg(test)]
mod tests {
    use super::supported_parquet_options;
    use datafusion::common::config::TableParquetOptions;

    #[test]
    fn parquet_options_allow_reader_only_schema_flags() -> Result<(), Box<dyn std::error::Error>> {
        let mut options = TableParquetOptions::default();
        options.global.binary_as_string = true;

        assert!(supported_parquet_options(&options));
        Ok(())
    }
}
