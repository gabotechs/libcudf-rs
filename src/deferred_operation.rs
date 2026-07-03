use crate::CuDFExecutionContext;
use std::marker::PhantomData;

/// A deferred cuDF operation that runs through an execution context.
///
/// Operations are created by public root-crate APIs and are inert until they are
/// submitted to [`CuDFExecutionContext::execute`].
///
/// Methods that do not return `CuDFOperation` are immediate and should be
/// limited to metadata, validation, or view construction.
///
/// ```no_run
/// use arrow::array::Int32Array;
/// use libcudf_rs::{CuDFColumn, CuDFExecutionContext};
///
/// let array = Int32Array::from(vec![1, 2, 3]);
/// let op = CuDFColumn::from_arrow_host(&array);
/// let column = CuDFExecutionContext::try_new_non_blocking()?.execute(op)?;
/// # Ok::<(), libcudf_rs::CuDFError>(())
/// ```
///
/// The trait is sealed: callers can execute operations produced by libcudf-rs,
/// but cannot implement their own.
pub trait CuDFOperation: operation_impl::CuDFOperationImpl {
    /// Value produced by the operation.
    type Output;
}

pub(crate) mod operation_impl {
    use crate::{CuDFExecutionContext, CuDFOperation, Result};

    /// Internal execution hook for operation values produced by this crate.
    pub trait CuDFOperationImpl {
        fn execute_on_context(
            self,
            ctx: &CuDFExecutionContext,
        ) -> Result<<Self as CuDFOperation>::Output>
        where
            Self: CuDFOperation;
    }
}

/// Executes an operation on `ctx`.
pub(crate) fn execute_on_context<O>(
    operation: O,
    ctx: &CuDFExecutionContext,
) -> crate::Result<O::Output>
where
    O: CuDFOperation,
{
    operation_impl::CuDFOperationImpl::execute_on_context(operation, ctx)
}

/// Wraps a one-shot closure as a cuDF operation.
pub(crate) fn deferred<Output, F>(execute: F) -> DeferredOperation<F, Output>
where
    F: FnOnce(&CuDFExecutionContext) -> crate::Result<Output>,
{
    DeferredOperation {
        execute,
        _output: PhantomData,
    }
}

/// Closure-backed implementation of [`CuDFOperation`].
pub(crate) struct DeferredOperation<F, Output> {
    execute: F,
    // `Output` is part of the operation type but is not stored as a field.
    _output: PhantomData<fn() -> Output>,
}

impl<Output, F> CuDFOperation for DeferredOperation<F, Output>
where
    F: FnOnce(&CuDFExecutionContext) -> crate::Result<Output>,
{
    type Output = Output;
}

impl<Output, F> operation_impl::CuDFOperationImpl for DeferredOperation<F, Output>
where
    F: FnOnce(&CuDFExecutionContext) -> crate::Result<Output>,
{
    fn execute_on_context(
        self,
        ctx: &CuDFExecutionContext,
    ) -> crate::Result<<Self as CuDFOperation>::Output> {
        (self.execute)(ctx)
    }
}
