use crate::aggregate::CuDFAggregationOp;
use arrow_schema::{DataType, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Accumulator, Documentation, GroupsAccumulator, Signature};
use datafusion_expr::expr::{AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs,
};
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::{AggregateUDFImpl, ReversedUDAF, SetMonotonicity, StatisticsArgs};
use std::any::Any;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
pub struct CuDFAggregateUDF {
    inner: Arc<dyn AggregateUDFImpl>,
    gpu: Arc<dyn CuDFAggregationOp>,
}

impl CuDFAggregateUDF {
    pub fn new(inner: Arc<dyn AggregateUDFImpl>, gpu: Arc<dyn CuDFAggregationOp>) -> Self {
        Self { inner, gpu }
    }

    pub fn gpu(&self) -> &Arc<dyn CuDFAggregationOp> {
        &self.gpu
    }
}

impl PartialEq for CuDFAggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.inner.dyn_eq(other.inner.as_any())
    }
}

impl Eq for CuDFAggregateUDF {}

impl Hash for CuDFAggregateUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state)
    }
}

impl fmt::Display for CuDFAggregateUDF {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl AggregateUDFImpl for CuDFAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn schema_name(&self, params: &AggregateFunctionParams) -> datafusion::common::Result<String> {
        self.inner.schema_name(params)
    }

    fn human_display(
        &self,
        params: &AggregateFunctionParams,
    ) -> datafusion::common::Result<String> {
        self.inner.human_display(params)
    }

    fn window_function_schema_name(
        &self,
        params: &WindowFunctionParams,
    ) -> datafusion::common::Result<String> {
        self.inner.window_function_schema_name(params)
    }

    fn display_name(&self, params: &AggregateFunctionParams) -> datafusion::common::Result<String> {
        self.inner.display_name(params)
    }

    fn window_function_display_name(
        &self,
        params: &WindowFunctionParams,
    ) -> datafusion::common::Result<String> {
        self.inner.window_function_display_name(params)
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> datafusion::common::Result<FieldRef> {
        self.inner.return_field(arg_fields)
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> datafusion::common::Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> datafusion::common::Result<Option<Arc<dyn AggregateUDFImpl>>> {
        self.inner
            .clone()
            .with_beneficial_ordering(beneficial_ordering)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::common::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }

    fn default_value(&self, data_type: &DataType) -> datafusion::common::Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }

    fn supports_within_group_clause(&self) -> bool {
        self.inner.supports_within_group_clause()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}
