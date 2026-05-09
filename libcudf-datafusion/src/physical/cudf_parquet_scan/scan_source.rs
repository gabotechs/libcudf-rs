use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::listing::{FileRange, PartitionedFile};
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccessPlanFilter};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

/// Local Parquet file accepted by the v1 cuDF scan.
#[derive(Debug, Clone)]
pub(crate) struct CuDFParquetSource {
    /// Local filesystem path.
    pub(crate) path: PathBuf,
    pub(crate) byte_len: usize,
    pub(crate) row_groups: RowGroupSelection,
    metadata: Arc<OnceLock<Arc<ParquetMetaData>>>,
}

/// Parquet row groups selected for one source file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowGroupSelection {
    /// Read every row group from the file.
    All,
    /// Read only these row group indices.
    Indices(Vec<i32>),
}

/// Reason a DataFusion Parquet file could not become a cuDF scan source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CuDFParquetSourceError {
    /// Partition columns would need host-side value materialization.
    PartitionValues,
    /// File extensions may carry access plans that v1 does not map to cuDF.
    FileExtensions,
    /// File size does not fit this platform's `usize`.
    FileSizeOverflow,
    /// Parquet metadata could not be read.
    FileMetadata,
    /// A Parquet row group index does not fit cuDF's `size_type`.
    RowGroupIndexOverflow,
    /// File byte range does not fit this platform's `usize`.
    FileRangeOverflow,
}

/// Converts DataFusion Parquet files into cuDF scan sources.
#[derive(Debug, Default)]
pub(crate) struct CuDFParquetSourceBuilder {
    metadata_by_path: HashMap<PathBuf, Arc<OnceLock<Arc<ParquetMetaData>>>>,
}

impl CuDFParquetSource {
    fn with_metadata_cache(
        path: PathBuf,
        byte_len: usize,
        row_groups: RowGroupSelection,
        metadata: Arc<OnceLock<Arc<ParquetMetaData>>>,
    ) -> Self {
        Self {
            path,
            byte_len,
            row_groups,
            metadata,
        }
    }

    pub(crate) fn byte_len(&self) -> usize {
        if self.byte_len > 0 {
            return self.byte_len;
        }
        std::fs::metadata(&self.path)
            .ok()
            .and_then(|metadata| usize::try_from(metadata.len()).ok())
            .unwrap_or(0)
    }

    pub(crate) fn row_group_selection(&self) -> &RowGroupSelection {
        &self.row_groups
    }

    pub(crate) fn metadata(&self) -> Result<Arc<ParquetMetaData>> {
        parquet_metadata_cached(&self.path, &self.metadata)
    }
}

impl CuDFParquetSourceBuilder {
    pub(crate) fn try_from_partitioned_file(
        &mut self,
        file: &PartitionedFile,
    ) -> std::result::Result<CuDFParquetSource, CuDFParquetSourceError> {
        if !file.partition_values.is_empty() {
            return Err(CuDFParquetSourceError::PartitionValues);
        }
        if file.extensions.is_some() {
            return Err(CuDFParquetSourceError::FileExtensions);
        }

        let path = local_path(file.object_meta.location.as_ref());
        let row_groups = file
            .range
            .as_ref()
            .map(|range| self.row_groups_in_range(&path, range))
            .transpose()?
            .map(RowGroupSelection::Indices)
            .unwrap_or(RowGroupSelection::All);
        let byte_len = partitioned_file_byte_len(file)?;

        let metadata = self.metadata_cache(&path);
        Ok(CuDFParquetSource::with_metadata_cache(
            path, byte_len, row_groups, metadata,
        ))
    }

    fn row_groups_in_range(
        &mut self,
        path: &Path,
        range: &FileRange,
    ) -> std::result::Result<Vec<i32>, CuDFParquetSourceError> {
        let metadata = self.metadata(path)?;
        row_groups_in_range(&metadata, range)
    }

    fn metadata(
        &mut self,
        path: &Path,
    ) -> std::result::Result<Arc<ParquetMetaData>, CuDFParquetSourceError> {
        parquet_metadata_cached(path, &self.metadata_cache(path))
            .map_err(|_| CuDFParquetSourceError::FileMetadata)
    }

    fn metadata_cache(&mut self, path: &Path) -> Arc<OnceLock<Arc<ParquetMetaData>>> {
        Arc::clone(
            self.metadata_by_path
                .entry(path.to_path_buf())
                .or_insert_with(|| Arc::new(OnceLock::new())),
        )
    }
}

impl RowGroupSelection {
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Indices(indices) if indices.is_empty())
    }

    pub(crate) fn indices(&self) -> Option<&[i32]> {
        match self {
            Self::All => None,
            Self::Indices(indices) => Some(indices),
        }
    }
}

fn local_path(location: &str) -> PathBuf {
    if location.starts_with('/') {
        location.into()
    } else {
        PathBuf::from("/").join(location)
    }
}

fn parquet_metadata(path: &Path) -> Result<Arc<ParquetMetaData>> {
    let file = File::open(path).map_err(|err| {
        DataFusionError::Execution(format!(
            "failed to open parquet metadata for {}: {err}",
            path.display()
        ))
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(Arc::clone(reader.metadata()))
}

fn parquet_metadata_cached(
    path: &Path,
    cache: &OnceLock<Arc<ParquetMetaData>>,
) -> Result<Arc<ParquetMetaData>> {
    if let Some(metadata) = cache.get() {
        return Ok(Arc::clone(metadata));
    }

    let metadata = parquet_metadata(path)?;
    let _ = cache.set(Arc::clone(&metadata));
    Ok(cache
        .get()
        .map_or(metadata, |metadata| Arc::clone(metadata)))
}

fn partitioned_file_byte_len(
    file: &PartitionedFile,
) -> std::result::Result<usize, CuDFParquetSourceError> {
    match &file.range {
        Some(range) => {
            let byte_len = range
                .end
                .checked_sub(range.start)
                .filter(|byte_len| *byte_len >= 0)
                .ok_or(CuDFParquetSourceError::FileRangeOverflow)?;
            usize::try_from(byte_len).map_err(|_| CuDFParquetSourceError::FileRangeOverflow)
        }
        None => usize::try_from(file.object_meta.size)
            .map_err(|_| CuDFParquetSourceError::FileSizeOverflow),
    }
}

fn row_groups_in_range(
    metadata: &ParquetMetaData,
    range: &FileRange,
) -> std::result::Result<Vec<i32>, CuDFParquetSourceError> {
    let access_plan = ParquetAccessPlan::new_all(metadata.num_row_groups());
    let mut filter = RowGroupAccessPlanFilter::new(access_plan);
    filter.prune_by_range(metadata.row_groups(), range);

    filter
        .build()
        .row_group_indexes()
        .into_iter()
        .map(|index| {
            i32::try_from(index).map_err(|_| CuDFParquetSourceError::RowGroupIndexOverflow)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{partitioned_file_byte_len, CuDFParquetSourceBuilder, RowGroupSelection};
    use datafusion::datasource::listing::PartitionedFile;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn partitioned_file_byte_len_uses_file_range() -> Result<(), Box<dyn std::error::Error>> {
        let path = weather_file("result-000000.parquet");
        let size = std::fs::metadata(&path)?.len();
        let file = PartitionedFile::new(path.to_string_lossy(), size).with_range(10, 25);
        let byte_len = partitioned_file_byte_len(&file)
            .map_err(|err| std::io::Error::other(format!("{err:?}")))?;

        assert_eq!(byte_len, 15);
        assert_eq!(RowGroupSelection::Indices(vec![]).indices(), Some(&[][..]));
        Ok(())
    }

    #[test]
    fn source_metadata_is_loaded_once() -> Result<(), Box<dyn std::error::Error>> {
        let path = weather_file("result-000000.parquet");
        let source = source_for_path(&path)?;

        let first = source.metadata()?;
        let second = source.metadata()?;

        assert!(Arc::ptr_eq(&first, &second));
        Ok(())
    }

    #[test]
    fn builder_sources_share_metadata_cache() -> Result<(), Box<dyn std::error::Error>> {
        let path = weather_file("result-000000.parquet");
        let mut builder = CuDFParquetSourceBuilder::default();

        let first = builder
            .try_from_partitioned_file(&partitioned_file(&path)?)
            .map_err(|err| std::io::Error::other(format!("{err:?}")))?;
        let second = builder
            .try_from_partitioned_file(&partitioned_file(&path)?)
            .map_err(|err| std::io::Error::other(format!("{err:?}")))?;

        assert!(Arc::ptr_eq(&first.metadata()?, &second.metadata()?));
        Ok(())
    }

    fn source_for_path(
        path: &PathBuf,
    ) -> Result<super::CuDFParquetSource, Box<dyn std::error::Error>> {
        let mut builder = CuDFParquetSourceBuilder::default();
        builder
            .try_from_partitioned_file(&partitioned_file(path)?)
            .map_err(|err| std::io::Error::other(format!("{err:?}")).into())
    }

    fn partitioned_file(path: &PathBuf) -> Result<PartitionedFile, Box<dyn std::error::Error>> {
        let size = std::fs::metadata(path)?.len();
        Ok(PartitionedFile::new(path.to_string_lossy(), size))
    }

    fn weather_file(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../testdata/weather/{name}"))
            .canonicalize()
            .expect("weather test file should exist")
    }
}
