use crate::column::CuDFColumn;
use crate::CuDFTable;
use std::sync::Arc;

pub trait CuDFRef: Send + Sync + 'static {}

impl CuDFRef for CuDFTable {}
impl CuDFRef for CuDFColumn {}

impl CuDFRef for Vec<Arc<dyn CuDFRef>> {}
