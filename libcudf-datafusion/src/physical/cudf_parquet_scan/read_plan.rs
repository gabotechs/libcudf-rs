use super::CuDFParquetSource;
use std::sync::Arc;

/// Precomputed file layout for a cuDF Parquet scan.
#[derive(Debug, Clone)]
pub(super) struct ReadPlan {
    partitions: Arc<[ReadPartition]>,
    read_columns: Option<Arc<[String]>>,
    file_count: usize,
    files_per_batch: usize,
}

/// One output partition in the scan.
#[derive(Debug, Clone)]
pub(super) struct ReadPartition {
    batches: Arc<[FileBatch]>,
}

/// Files read together by one cuDF Parquet call.
#[derive(Debug, Clone)]
pub(super) struct FileBatch {
    sources: Arc<[CuDFParquetSource]>,
    file_bytes: usize,
}

impl ReadPlan {
    pub(super) fn new(
        file_groups: Vec<Vec<CuDFParquetSource>>,
        read_columns: Option<Arc<[String]>>,
        files_per_batch: usize,
    ) -> Self {
        let file_count = Self::source_count(&file_groups);
        let files_per_batch = files_per_batch.min(file_count);
        let partitions = file_groups
            .into_iter()
            .map(|sources| ReadPartition::new(&sources, files_per_batch))
            .collect::<Vec<_>>()
            .into();

        Self {
            partitions,
            read_columns,
            file_count,
            files_per_batch,
        }
    }

    pub(super) fn source_count(file_groups: &[Vec<CuDFParquetSource>]) -> usize {
        file_groups.iter().map(Vec::len).sum()
    }

    pub(super) fn repartitioned_source_groups(
        file_groups: &[Vec<CuDFParquetSource>],
        target_partitions: usize,
    ) -> Option<Vec<Vec<CuDFParquetSource>>> {
        if target_partitions <= file_groups.len() {
            return None;
        }

        let files = file_groups.iter().flatten().cloned().collect::<Vec<_>>();
        let partition_count = target_partitions.min(files.len());
        if partition_count <= file_groups.len() {
            return None;
        }

        let mut file_groups = vec![Vec::new(); partition_count];
        for (index, file) in files.into_iter().enumerate() {
            file_groups[index % partition_count].push(file);
        }
        Some(file_groups)
    }

    pub(super) fn partition(&self, index: usize) -> Option<ReadPartition> {
        self.partitions.get(index).cloned()
    }

    pub(super) fn batch_count(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.batches.len())
            .sum()
    }

    pub(super) fn file_count(&self) -> usize {
        self.file_count
    }

    pub(super) fn files_per_batch(&self) -> usize {
        self.files_per_batch
    }

    pub(super) fn read_columns(&self) -> Option<Arc<[String]>> {
        self.read_columns.clone()
    }

    pub(super) fn read_column_count(&self) -> Option<usize> {
        self.read_columns.as_ref().map(|columns| columns.len())
    }
}

impl ReadPartition {
    fn new(sources: &[CuDFParquetSource], files_per_batch: usize) -> Self {
        let batches = sources
            .chunks(files_per_batch)
            .map(FileBatch::new)
            .collect::<Vec<_>>()
            .into();
        Self { batches }
    }

    pub(super) fn batches(&self) -> &[FileBatch] {
        &self.batches
    }
}

impl FileBatch {
    fn new(sources: &[CuDFParquetSource]) -> Self {
        let file_bytes = sources.iter().map(CuDFParquetSource::byte_len).sum();

        Self {
            sources: Arc::from(sources.to_vec()),
            file_bytes,
        }
    }

    pub(super) fn sources(&self) -> &[CuDFParquetSource] {
        &self.sources
    }

    pub(super) fn file_bytes(&self) -> usize {
        self.file_bytes
    }
}
