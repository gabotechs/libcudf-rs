use super::read_plan::FileBatch;
use super::{CuDFParquetSource, RowGroupSelection};
use crate::errors::cudf_to_df;
use crate::physical::normalize_scalar_for_cudf;
use arrow_schema::SchemaRef;
use datafusion::common::{plan_err, DataFusionError};
use datafusion::scalar::ScalarValue;
use libcudf_rs::{
    CuDFAstExpression, CuDFColumn, CuDFError, CuDFParquetReadOptions, CuDFParquetReadResult,
    CuDFScalar, CuDFStream, CuDFTable,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Reads cuDF Parquet batches and aligns them to the scan output schema.
pub(super) struct ParquetBatchReader {
    schema: SchemaRef,
    read_columns: Option<Arc<[String]>>,
    filter: Option<Arc<CuDFAstExpression>>,
    chunk_read_limit: usize,
    pass_read_limit: usize,
    stream: CuDFStream,
}

/// cuDF columns plus row count for one output batch.
pub(super) struct ReadBatch {
    pub(super) columns: Vec<CuDFColumn>,
    pub(super) num_rows: usize,
}

enum ChunkReadOutcome {
    Finished { emitted: bool, continued: bool },
    CudfErrorBeforeEmit(CuDFError),
}

impl ParquetBatchReader {
    pub(super) fn new(
        schema: SchemaRef,
        read_columns: Option<Arc<[String]>>,
        filter: Option<Arc<CuDFAstExpression>>,
        chunk_read_limit: usize,
        pass_read_limit: usize,
        stream: CuDFStream,
    ) -> Self {
        Self {
            schema,
            read_columns,
            filter,
            chunk_read_limit,
            pass_read_limit,
            stream,
        }
    }

    pub(super) fn read<F>(&self, batch: &FileBatch, mut emit: F) -> datafusion::common::Result<bool>
    where
        F: FnMut(ReadBatch) -> datafusion::common::Result<bool>,
    {
        let read_columns = self.read_columns.as_deref();
        let filter = self.filter.as_deref();
        if batch_has_no_selected_row_groups(batch) {
            return emit(empty_read_batch(&self.schema, &self.stream)?);
        }

        if self.schema.fields().is_empty() && filter.is_none() {
            return emit(ReadBatch {
                columns: vec![],
                num_rows: parquet_batch_row_count(batch)?,
            });
        }

        if batch_has_missing_read_columns(batch, read_columns, &self.schema)? {
            return self.read_sources_individually(batch, &mut emit);
        }

        self.read_sources_together(batch, &mut emit)
    }
}

struct ParquetChunkReadArgs<'a, P> {
    paths: &'a [P],
    row_groups: Option<&'a [Vec<i32>]>,
    read_columns: Option<&'a [String]>,
    filter: Option<&'a CuDFAstExpression>,
    chunk_read_limit: usize,
    pass_read_limit: usize,
    schema: &'a SchemaRef,
    stream: &'a CuDFStream,
}

fn read_parquet_chunks<P>(
    args: ParquetChunkReadArgs<'_, P>,
    emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
) -> datafusion::common::Result<ChunkReadOutcome>
where
    P: AsRef<Path>,
{
    let ParquetChunkReadArgs {
        paths,
        row_groups,
        read_columns,
        filter,
        chunk_read_limit,
        pass_read_limit,
        schema,
        stream,
    } = args;
    let mut emitted = false;
    let mut continued = true;
    let mut callback_error = None;
    let read_result = CuDFTable::for_each_parquet_chunk_on_stream(
        CuDFParquetReadOptions {
            paths,
            columns: read_columns,
            row_groups,
            filter,
            allow_mismatched_pq_schemas: read_columns.is_some(),
            ignore_missing_columns: true,
        },
        chunk_read_limit,
        pass_read_limit,
        stream,
        |read| {
            emitted = true;
            match align_parquet_read(read, schema).and_then(&mut *emit) {
                Ok(keep_reading) => {
                    continued = keep_reading;
                    Ok(keep_reading)
                }
                Err(err) => {
                    continued = false;
                    callback_error = Some(err);
                    Ok(false)
                }
            }
        },
    );

    if let Some(err) = callback_error {
        return Err(err);
    }
    match read_result {
        Ok(()) => Ok(ChunkReadOutcome::Finished { emitted, continued }),
        Err(err) if !emitted => Ok(ChunkReadOutcome::CudfErrorBeforeEmit(err)),
        Err(err) => Err(cudf_to_df(err)),
    }
}

impl ParquetBatchReader {
    fn read_sources_together(
        &self,
        batch: &FileBatch,
        emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
    ) -> datafusion::common::Result<bool> {
        if let [source] = batch.sources() {
            return self.read_source(source, emit);
        }
        let files = batch
            .sources()
            .iter()
            .map(|source| &source.path)
            .collect::<Vec<_>>();
        let row_groups = explicit_row_groups_for_batch(batch)?;

        match read_parquet_chunks(
            ParquetChunkReadArgs {
                paths: &files,
                row_groups: row_groups.as_deref(),
                read_columns: self.read_columns.as_deref(),
                filter: self.filter.as_deref(),
                chunk_read_limit: self.chunk_read_limit,
                pass_read_limit: self.pass_read_limit,
                schema: &self.schema,
                stream: &self.stream,
            },
            emit,
        )? {
            ChunkReadOutcome::Finished { emitted, continued } => {
                if !emitted {
                    return emit(empty_read_batch(&self.schema, &self.stream)?);
                }
                Ok(continued)
            }
            ChunkReadOutcome::CudfErrorBeforeEmit(_) => {
                // Multi-file cuDF reads can fail when files have schema differences
                // that are still valid under DataFusion's schema adaptation rules.
                // Retry per source so missing columns can be null-filled independently.
                self.read_sources_individually(batch, emit)
            }
        }
    }
}

fn explicit_row_groups_for_batch(
    batch: &FileBatch,
) -> datafusion::common::Result<Option<Vec<Vec<i32>>>> {
    // cuDF expects either no row-group argument for all row groups in every file,
    // or one explicit row-group vector per file. If any source is pruned, expand
    // unpruned sources to explicit indices so mixed batches stay aligned.
    if !batch
        .sources()
        .iter()
        .any(|source| !matches!(source.row_group_selection(), RowGroupSelection::All))
    {
        return Ok(None);
    }

    batch
        .sources()
        .iter()
        .map(explicit_row_groups_for_source)
        .collect::<datafusion::common::Result<Vec<_>>>()
        .map(Some)
}

fn explicit_row_groups_for_source(
    source: &CuDFParquetSource,
) -> datafusion::common::Result<Vec<i32>> {
    match source.row_group_selection() {
        RowGroupSelection::All => all_row_group_indices(source),
        RowGroupSelection::Indices(indices) => Ok(indices.clone()),
    }
}

fn all_row_group_indices(source: &CuDFParquetSource) -> datafusion::common::Result<Vec<i32>> {
    let metadata = source.metadata()?;
    (0..metadata.num_row_groups())
        .map(|index| {
            i32::try_from(index).map_err(|_| {
                DataFusionError::Execution(
                    "CuDFParquetScanExec row group index overflowed i32".to_string(),
                )
            })
        })
        .collect()
}

fn batch_has_no_selected_row_groups(batch: &FileBatch) -> bool {
    batch
        .sources()
        .iter()
        .all(|source| source.row_group_selection().is_empty())
}

fn batch_has_missing_read_columns(
    batch: &FileBatch,
    read_columns: Option<&[String]>,
    schema: &SchemaRef,
) -> datafusion::common::Result<bool> {
    if batch.sources().len() <= 1 {
        return Ok(false);
    }

    let schema_columns;
    let required_columns = match read_columns {
        Some(columns) => columns,
        None => {
            schema_columns = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            schema_columns.as_slice()
        }
    };
    if required_columns.is_empty() {
        return Ok(false);
    }

    batch.sources().iter().try_fold(false, |missing, source| {
        if missing {
            return Ok(true);
        }
        let available_columns = source.available_columns()?;
        Ok(required_columns
            .iter()
            .any(|column| !available_columns.contains(column.as_str())))
    })
}

fn empty_read_batch(
    schema: &SchemaRef,
    stream: &CuDFStream,
) -> datafusion::common::Result<ReadBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| null_column(field.data_type(), 0, stream))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    Ok(ReadBatch {
        columns,
        num_rows: 0,
    })
}

impl ParquetBatchReader {
    fn read_source(
        &self,
        source: &CuDFParquetSource,
        emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
    ) -> datafusion::common::Result<bool> {
        let row_groups = source
            .row_group_selection()
            .indices()
            .map(|indices| vec![indices.to_vec()]);
        match read_parquet_chunks(
            ParquetChunkReadArgs {
                paths: std::slice::from_ref(&source.path),
                row_groups: row_groups.as_deref(),
                read_columns: self.read_columns.as_deref(),
                filter: self.filter.as_deref(),
                chunk_read_limit: self.chunk_read_limit,
                pass_read_limit: self.pass_read_limit,
                schema: &self.schema,
                stream: &self.stream,
            },
            emit,
        )? {
            ChunkReadOutcome::Finished { emitted, continued } => {
                if !emitted {
                    return emit(empty_read_batch(&self.schema, &self.stream)?);
                }
                Ok(continued)
            }
            ChunkReadOutcome::CudfErrorBeforeEmit(err) => Err(cudf_to_df(err)),
        }
    }

    fn read_sources_individually(
        &self,
        batch: &FileBatch,
        emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
    ) -> datafusion::common::Result<bool> {
        let mut emitted = false;
        for source in batch.sources() {
            if source.row_group_selection().is_empty() {
                continue;
            }

            let continued = self.read_source(source, &mut |read| {
                emitted = true;
                emit(read)
            })?;
            if !continued {
                return Ok(false);
            }
        }

        if !emitted {
            return emit(empty_read_batch(&self.schema, &self.stream)?);
        }
        Ok(true)
    }
}

fn align_parquet_read(
    read: CuDFParquetReadResult,
    schema: &SchemaRef,
) -> datafusion::common::Result<ReadBatch> {
    let stream = read.table.execution_stream();
    let columns = read.table.into_columns().map_err(cudf_to_df)?;
    if read.column_names.len() != columns.len() {
        return plan_err!(
            "cuDF returned {} parquet column names for {} columns",
            read.column_names.len(),
            columns.len()
        );
    }

    let mut columns_by_name = HashMap::with_capacity(columns.len());
    for (name, column) in read.column_names.into_iter().zip(columns) {
        if columns_by_name.insert(name.clone(), column).is_some() {
            return plan_err!("cuDF parquet read returned duplicate column name '{name}'");
        }
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = match columns_by_name.remove(field.name().as_str()) {
            Some(column) => column,
            None => null_column(field.data_type(), read.num_rows, &stream)?,
        };
        columns.push(column);
    }

    Ok(ReadBatch {
        columns,
        num_rows: read.num_rows,
    })
}

fn null_column(
    data_type: &arrow_schema::DataType,
    num_rows: usize,
    stream: &CuDFStream,
) -> datafusion::common::Result<CuDFColumn> {
    let scalar = normalize_scalar_for_cudf(ScalarValue::try_new_null(data_type)?)?;
    let scalar = CuDFScalar::try_from_arrow_host_on_stream(scalar.to_scalar()?, stream)
        .map_err(cudf_to_df)?;
    CuDFColumn::try_from_scalar_on_stream(&scalar, num_rows, stream).map_err(cudf_to_df)
}

fn parquet_batch_row_count(batch: &FileBatch) -> datafusion::common::Result<usize> {
    batch.sources().iter().try_fold(0usize, |rows, source| {
        rows.checked_add(parquet_source_row_count(source)?)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "CuDFParquetScanExec row count overflowed usize".to_string(),
                )
            })
    })
}

fn parquet_source_row_count(source: &CuDFParquetSource) -> datafusion::common::Result<usize> {
    let metadata = source.metadata()?;
    let row_count = match source.row_group_selection() {
        RowGroupSelection::Indices(row_groups) => {
            row_groups.iter().try_fold(0i64, |rows, &index| {
                let row_group = usize::try_from(index).map_err(|_| {
                    DataFusionError::Execution(
                        "CuDFParquetScanExec row group index was negative".to_string(),
                    )
                })?;
                let row_group = metadata.row_group(row_group);
                rows.checked_add(row_group.num_rows()).ok_or_else(|| {
                    DataFusionError::Execution(
                        "CuDFParquetScanExec row count overflowed i64".to_string(),
                    )
                })
            })?
        }
        RowGroupSelection::All => metadata.file_metadata().num_rows(),
    };

    usize::try_from(row_count)
        .map_err(|_| DataFusionError::Execution("Parquet file row count was negative".to_string()))
}
