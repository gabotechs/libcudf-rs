use crate::errors::cudf_to_df;
use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::common::types::{
    logical_float64, logical_int16, logical_int32, logical_int64, logical_int8, logical_uint16,
    logical_uint32, logical_uint64, logical_uint8, NativeType,
};
use datafusion::error::Result;
use datafusion::logical_expr::{
    Coercion, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use libcudf_rs::{AggregationOp, AggregationRequest, CuDFColumnView};
use std::any::type_name;
use std::fmt::Debug;
use crate::aggregate::CuDFAggregationOp;

#[derive(Debug)]
pub struct CuDFSum {
    signature: Signature,
}

impl CuDFSum {
    pub fn new() -> Self {
        Self {
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            // smallint, int, bigint, real, double precision, decimal, or interval.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    // Unsigned to u64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_uint64()),
                        vec![
                            TypeSignatureClass::Native(logical_uint8()),
                            TypeSignatureClass::Native(logical_uint16()),
                            TypeSignatureClass::Native(logical_uint32()),
                        ],
                        NativeType::UInt64,
                    )]),
                    // Signed to i64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![
                            TypeSignatureClass::Native(logical_int8()),
                            TypeSignatureClass::Native(logical_int16()),
                            TypeSignatureClass::Native(logical_int32()),
                        ],
                        NativeType::Int64,
                    )]),
                    // Floats to f64
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Float],
                        NativeType::Float64,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Duration,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl CuDFAggregationOp for CuDFSum {
    fn name(&self) -> &str {
        type_name::<Self>()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, got {}", args.len());
        }

        // cuDF's SUM aggregation always returns signed types for integer inputs
        match &args[0] {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(DataType::Int64),
            DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
            dt @ DataType::Decimal128(_, _) | dt @ DataType::Decimal256(_, _) => Ok(dt.clone()),
            dt @ (DataType::Duration(_) | DataType::Interval(_)) => Ok(dt.clone()),
            dt => exec_err!("SUM does not support type {:?}", dt),
        }
    }

    fn partial_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        self.final_requests(args)
    }

    fn final_requests(&self, args: &[CuDFColumnView]) -> Result<Vec<AggregationRequest>> {
        if args.len() != 1 {
            return exec_err!("SUM expects 1 argument, got {}", args.len());
        }

        let view = CuDFColumnView::from_arrow(&args[0]).map_err(cudf_to_df)?;
        let mut request = AggregationRequest::new(&view);
        request.add(AggregationOp::SUM.group_by());

        Ok(vec![request])
    }

    fn merge(&self, args: &[CuDFColumnView]) -> Result<CuDFColumnView> {
        if args.len() != 1 {
            return exec_err!("SUM merge expects 1 argument, got {}", args.len());
        }
        Ok(args[0].clone())
    }
}
