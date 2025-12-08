use crate::column::CuDFColumn;
use crate::{CuDFColumnView, CuDFTable, CuDFTableView};
use std::sync::Arc;

pub trait CuDFRef: Send + Sync + 'static {}

impl CuDFRef for CuDFTable {}
impl CuDFRef for CuDFTableView {}
impl CuDFRef for CuDFColumn {}
impl CuDFRef for CuDFColumnView {}

impl CuDFRef for Vec<Arc<dyn CuDFRef>> {}
