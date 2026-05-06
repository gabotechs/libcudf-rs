use crate::CuDFConfig;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use libcudf_rs::CuDFStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Query-scoped cuDF runtime state attached to a [`TaskContext`].
///
/// Holds the CUDA stream assigned to each active GPU segment + partition.
/// Streams are looked up by `(segment_id, partition)` so that a query plan
/// containing multiple GPU segments separated by CPU repartition boundaries
/// can use a fresh stream per segment.
#[derive(Default)]
pub struct CuDFTaskContext {
    streams: Mutex<HashMap<(usize, usize), Arc<CuDFStream>>>,
}

impl CuDFTaskContext {
    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Result<Arc<Self>, DataFusionError> {
        ctx.session_config().get_extension::<Self>().ok_or_else(|| {
            DataFusionError::Internal(
                "CuDFTaskContext extension not installed in TaskContext".into(),
            )
        })
    }

    pub fn stream(&self, segment_id: usize, partition: usize) -> Option<Arc<CuDFStream>> {
        self.streams
            .lock()
            .expect("CuDFTaskContext mutex poisoned")
            .get(&(segment_id, partition))
            .cloned()
    }

    pub fn set_stream(&self, segment_id: usize, partition: usize, stream: Arc<CuDFStream>) {
        self.streams
            .lock()
            .expect("CuDFTaskContext mutex poisoned")
            .insert((segment_id, partition), stream);
    }

    pub fn unset_stream(&self, segment_id: usize, partition: usize) -> Option<Arc<CuDFStream>> {
        self.streams
            .lock()
            .expect("CuDFTaskContext mutex poisoned")
            .remove(&(segment_id, partition))
    }
}

pub(crate) fn cuda_streams_enabled(ctx: &TaskContext) -> bool {
    ctx.session_config()
        .options()
        .extensions
        .get::<CuDFConfig>()
        .is_some_and(|cfg| cfg.cuda_streams)
}
