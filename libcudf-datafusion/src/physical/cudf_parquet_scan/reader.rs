use super::read_plan::FileBatch;
use super::{CuDFParquetSource, RowGroupSelection};
use crate::errors::cudf_to_df;
use crate::physical::normalize_scalar_for_cudf;
use arrow_schema::SchemaRef;
use datafusion::common::{plan_err, DataFusionError};
use datafusion::scalar::ScalarValue;
use libcudf_rs::{
    CuDFAstExpression, CuDFColumn, CuDFParquetReadOptions, CuDFParquetReadResult, CuDFScalar,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Reads cuDF Parquet batches and aligns them to the scan output schema.
pub(super) struct ParquetBatchReader {
    schema: SchemaRef,
    read_columns: Option<Arc<[String]>>,
    filter: Option<Arc<CuDFAstExpression>>,
    chunk_read_limit: usize,
    pass_read_limit: usize,
}

/// cuDF columns plus row count for one output batch.
pub(super) struct ReadBatch {
    pub(super) columns: Vec<CuDFColumn>,
    pub(super) num_rows: usize,
}

impl ParquetBatchReader {
    pub(super) fn new(
        schema: SchemaRef,
        read_columns: Option<Arc<[String]>>,
        filter: Option<Arc<CuDFAstExpression>>,
        chunk_read_limit: usize,
        pass_read_limit: usize,
    ) -> Self {
        Self {
            schema,
            read_columns,
            filter,
            chunk_read_limit,
            pass_read_limit,
        }
    }

    pub(super) fn read<F>(&self, batch: &FileBatch, mut emit: F) -> datafusion::common::Result<bool>
    where
        F: FnMut(ReadBatch) -> datafusion::common::Result<bool>,
    {
        let read_columns = self.read_columns.as_deref();
        let filter = self.filter.as_deref();
        if batch_has_no_selected_row_groups(batch) {
            return emit(empty_read_batch(&self.schema)?);
        }

        if self.schema.fields().is_empty() && filter.is_none() {
            return emit(ReadBatch {
                columns: vec![],
                num_rows: parquet_batch_row_count(batch)?,
            });
        }

        if batch_has_missing_read_columns(batch, read_columns, &self.schema)? {
            return read_parquet_sources_individually(
                batch,
                read_columns,
                filter,
                &self.schema,
                self.chunk_read_limit,
                self.pass_read_limit,
                &mut emit,
            );
        }

        read_parquet_sources_together(
            batch,
            read_columns,
            filter,
            self.chunk_read_limit,
            self.pass_read_limit,
            &self.schema,
            &mut emit,
        )
    }
}

fn read_parquet_sources_together(
    batch: &FileBatch,
    read_columns: Option<&[String]>,
    filter: Option<&CuDFAstExpression>,
    chunk_read_limit: usize,
    pass_read_limit: usize,
    schema: &SchemaRef,
    emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
) -> datafusion::common::Result<bool> {
    if let [source] = batch.sources() {
        return read_parquet_source(
            source,
            read_columns,
            filter,
            chunk_read_limit,
            pass_read_limit,
            schema,
            emit,
        );
    }
    let files = batch
        .sources()
        .iter()
        .map(|source| &source.path)
        .collect::<Vec<_>>();
    let row_groups = parquet_batch_row_groups(batch)?;
    let mut emitted = false;
    let mut continued = true;
    let mut callback_error = None;
    let read_result = libcudf_rs::CuDFTable::for_each_parquet_chunk(
        CuDFParquetReadOptions {
            paths: &files,
            columns: read_columns,
            row_groups: row_groups.as_deref(),
            filter,
            allow_mismatched_pq_schemas: read_columns.is_some(),
            ignore_missing_columns: true,
        },
        chunk_read_limit,
        pass_read_limit,
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
        Ok(()) => {}
        Err(err) if !emitted => {
            return read_parquet_sources_individually(
                batch,
                read_columns,
                filter,
                schema,
                chunk_read_limit,
                pass_read_limit,
                emit,
            );
        }
        Err(err) => return Err(cudf_to_df(err)),
    }
    if !emitted {
        return emit(empty_read_batch(schema)?);
    }
    Ok(continued)
}

fn parquet_batch_row_groups(
    batch: &FileBatch,
) -> datafusion::common::Result<Option<Vec<Vec<i32>>>> {
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
        .map(cudf_row_groups_for_source)
        .collect::<datafusion::common::Result<Vec<_>>>()
        .map(Some)
}

fn cudf_row_groups_for_source(source: &CuDFParquetSource) -> datafusion::common::Result<Vec<i32>> {
    match source.row_group_selection() {
        RowGroupSelection::All => all_source_row_groups(source),
        RowGroupSelection::Indices(indices) => Ok(indices.clone()),
    }
}

fn all_source_row_groups(source: &CuDFParquetSource) -> datafusion::common::Result<Vec<i32>> {
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

fn empty_read_batch(schema: &SchemaRef) -> datafusion::common::Result<ReadBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| null_column(field.data_type(), 0))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    Ok(ReadBatch {
        columns,
        num_rows: 0,
    })
}

fn read_parquet_source(
    source: &CuDFParquetSource,
    read_columns: Option<&[String]>,
    filter: Option<&CuDFAstExpression>,
    chunk_read_limit: usize,
    pass_read_limit: usize,
    schema: &SchemaRef,
    emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
) -> datafusion::common::Result<bool> {
    let row_groups = source
        .row_group_selection()
        .indices()
        .map(|indices| vec![indices.to_vec()]);
    let mut emitted = false;
    let mut continued = true;
    let mut callback_error = None;
    let read_result = libcudf_rs::CuDFTable::for_each_parquet_chunk(
        CuDFParquetReadOptions {
            paths: std::slice::from_ref(&source.path),
            columns: read_columns,
            row_groups: row_groups.as_deref(),
            filter,
            allow_mismatched_pq_schemas: read_columns.is_some(),
            ignore_missing_columns: true,
        },
        chunk_read_limit,
        pass_read_limit,
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
    read_result.map_err(cudf_to_df)?;
    if !emitted {
        return emit(empty_read_batch(schema)?);
    }
    Ok(continued)
}

fn read_parquet_sources_individually(
    batch: &FileBatch,
    read_columns: Option<&[String]>,
    filter: Option<&CuDFAstExpression>,
    schema: &SchemaRef,
    chunk_read_limit: usize,
    pass_read_limit: usize,
    emit: &mut impl FnMut(ReadBatch) -> datafusion::common::Result<bool>,
) -> datafusion::common::Result<bool> {
    let mut emitted = false;
    for source in batch.sources() {
        if source.row_group_selection().is_empty() {
            continue;
        }

        let continued = read_parquet_source(
            source,
            read_columns,
            filter,
            chunk_read_limit,
            pass_read_limit,
            schema,
            &mut |read| {
                emitted = true;
                emit(read)
            },
        )?;
        if !continued {
            return Ok(false);
        }
    }

    if !emitted {
        return emit(empty_read_batch(schema)?);
    }
    Ok(true)
}

fn align_parquet_read(
    read: CuDFParquetReadResult,
    schema: &SchemaRef,
) -> datafusion::common::Result<ReadBatch> {
    let columns = read.table.into_columns();
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
            None => null_column(field.data_type(), read.num_rows)?,
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
) -> datafusion::common::Result<CuDFColumn> {
    let scalar = normalize_scalar_for_cudf(ScalarValue::try_new_null(data_type)?)?;
    let scalar = CuDFScalar::from_arrow_host(scalar.to_scalar()?).map_err(cudf_to_df)?;
    CuDFColumn::from_scalar(&scalar, num_rows).map_err(cudf_to_df)
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
