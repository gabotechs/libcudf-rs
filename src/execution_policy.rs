use arrow::array::ArrayData;
use cxx::UniquePtr;
use libcudf_sys::ffi;
use std::sync::Arc;

use crate::keep_alive::CuDFKeepAlive;
use crate::stream_readiness::{CuDFStreamDependency, CuDFStreamReady};
use crate::{
    execution_context::CurrentDeviceGuard, CuDFColumn, CuDFColumnView, CuDFError,
    CuDFExecutionContext, CuDFScalar, CuDFTable, CuDFTableView, Result,
};

/// Starts a cuDF operation launch on `ctx`.
///
/// The launch owns the temporary device guard, stream view, captured memory
/// resource, and output keepalives.
pub(crate) fn launch(ctx: &CuDFExecutionContext) -> Result<OperationLaunch<'_>> {
    let device_guard = ctx.activate_device()?;
    let stream = ctx.stream_view()?;
    Ok(OperationLaunch {
        ctx,
        _device_guard: device_guard,
        stream,
        resource: ctx.resource()?,
        keepalives: Vec::new(),
    })
}

/// Per-operation launch state.
///
/// An operation creates this once, waits on every input it will read, calls the
/// sys binding with `stream()` and `resource()`, then marks outputs ready.
pub(crate) struct OperationLaunch<'a> {
    ctx: &'a CuDFExecutionContext,
    _device_guard: CurrentDeviceGuard,
    stream: UniquePtr<ffi::CudaStreamView>,
    resource: &'a ffi::DeviceAsyncResourceRef,
    keepalives: Vec<CuDFKeepAlive>,
}

impl OperationLaunch<'_> {
    /// Execution context that owns this launch.
    pub(crate) fn context(&self) -> &CuDFExecutionContext {
        self.ctx
    }

    /// CUDA stream view passed to sys bindings.
    pub(crate) fn stream(&self) -> Result<&ffi::CudaStreamView> {
        self.stream
            .as_ref()
            .ok_or(CuDFError::NullHandle("CUDA stream view"))
    }

    /// Device memory resource passed to sys bindings.
    pub(crate) fn resource(&self) -> &ffi::DeviceAsyncResourceRef {
        self.resource
    }

    /// Waits for a table input and retains it until the output is ready.
    pub(crate) fn wait_table(&mut self, table: &CuDFTableView) -> Result<()> {
        self.wait_ready(table)?;
        self.keepalives.push(CuDFKeepAlive::TableView {
            _table: table.clone(),
        });
        Ok(())
    }

    /// Waits for a column input and retains it until the output is ready.
    pub(crate) fn wait_column(&mut self, column: &CuDFColumnView) -> Result<()> {
        self.wait_ready(column)?;
        self.keepalives.push(CuDFKeepAlive::ColumnView {
            _column: column.clone(),
        });
        Ok(())
    }

    /// Waits for a scalar input and retains it until the output is ready.
    pub(crate) fn wait_scalar(&mut self, scalar: &CuDFScalar) -> Result<()> {
        self.wait_ready(scalar)?;
        self.keepalives.push(CuDFKeepAlive::Scalar {
            _scalar: scalar.clone(),
        });
        Ok(())
    }

    /// Retains Arrow array data borrowed by an in-flight Arrow FFI import.
    pub(crate) fn keep_arrow_array_data(&mut self, data: Arc<ArrayData>) {
        self.keepalives
            .push(CuDFKeepAlive::ArrowData { _data: data });
    }

    /// Retains an additional value until the output is ready.
    pub(crate) fn keep_alive(&mut self, keepalive: CuDFKeepAlive) {
        self.keepalives.push(keepalive);
    }

    /// Records output readiness and returns its dependency handle.
    pub(crate) fn into_stream_dependency(self) -> Result<CuDFStreamDependency> {
        let OperationLaunch {
            ctx,
            _device_guard: device_guard,
            stream,
            resource: _,
            mut keepalives,
        } = self;
        if let Some(stream) = ctx.stream_keepalive() {
            keepalives.push(CuDFKeepAlive::Stream { _stream: stream });
        }
        let dependency = CuDFStreamDependency::record_on_stream(
            stream
                .as_ref()
                .ok_or(CuDFError::NullHandle("CUDA stream view"))?,
            keepalives,
        )?;
        drop(device_guard);
        Ok(dependency)
    }

    /// Records readiness without consuming the launch.
    ///
    /// Used by producer APIs that return multiple outputs from one launch scope.
    pub(crate) fn record_stream_dependency(
        &self,
        mut keepalives: Vec<CuDFKeepAlive>,
    ) -> Result<CuDFStreamDependency> {
        if let Some(stream) = self.ctx.stream_keepalive() {
            keepalives.push(CuDFKeepAlive::Stream { _stream: stream });
        }
        CuDFStreamDependency::record_on_stream(self.stream()?, keepalives)
    }

    /// Marks a table output ready on this launch's stream.
    pub(crate) fn ready_table(self, table: CuDFTable) -> Result<CuDFTable> {
        let dependency = self.into_stream_dependency()?;
        Ok(table.with_stream_readiness(dependency))
    }

    /// Marks a column output ready on this launch's stream.
    pub(crate) fn ready_column(self, column: CuDFColumn) -> Result<CuDFColumn> {
        let dependency = self.into_stream_dependency()?;
        Ok(column.with_stream_readiness(dependency))
    }

    /// Marks a scalar output ready on this launch's stream.
    pub(crate) fn ready_scalar(self, scalar: CuDFScalar) -> Result<CuDFScalar> {
        let dependency = self.into_stream_dependency()?;
        Ok(scalar.with_stream_readiness(dependency))
    }

    fn wait_ready(&self, value: &impl CuDFStreamReady) -> Result<()> {
        value.wait_ready_on_stream(self.stream()?)
    }
}
