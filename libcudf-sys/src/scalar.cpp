#include "scalar.h"
#include "data_type.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/scalar/scalar.hpp>
#include <cudf/types.hpp>
#include <stdexcept>

namespace libcudf_bridge {
    // Scalar implementation
    Scalar::Scalar() : inner(nullptr) {
    }

    Scalar::~Scalar() = default;

    bool Scalar::is_valid() const {
        if (!inner) {
            return false;
        }
        return inner->is_valid();
    }

    [[nodiscard]] std::unique_ptr<DataType> Scalar::data_type() const {
        auto dtype = inner->type();
        auto type_id = static_cast<int32_t>(dtype.id());

        // Only pass scale for decimal types
        if (dtype.id() == cudf::type_id::DECIMAL32 ||
            dtype.id() == cudf::type_id::DECIMAL64 ||
            dtype.id() == cudf::type_id::DECIMAL128) {
            return std::make_unique<DataType>(type_id, dtype.scale());
        } else {
            return std::make_unique<DataType>(type_id);
        }
    }

    [[nodiscard]] std::unique_ptr<Scalar> Scalar::clone() const {
        auto cloned = std::make_unique<Scalar>();

        if (!inner) {
            return cloned;
        }

        // Get the scalar's type to determine which derived class to instantiate
        auto dtype = inner->type();

        // Create a new scalar of the same type using the polymorphic copy constructor
        // We use a switch to dispatch to the correct derived type's copy constructor
        switch (dtype.id()) {
            case cudf::type_id::INT8:
                cloned->inner = std::make_unique<cudf::numeric_scalar<int8_t>>(
                    static_cast<cudf::numeric_scalar<int8_t> const&>(*inner));
                break;
            case cudf::type_id::INT16:
                cloned->inner = std::make_unique<cudf::numeric_scalar<int16_t>>(
                    static_cast<cudf::numeric_scalar<int16_t> const&>(*inner));
                break;
            case cudf::type_id::INT32:
                cloned->inner = std::make_unique<cudf::numeric_scalar<int32_t>>(
                    static_cast<cudf::numeric_scalar<int32_t> const&>(*inner));
                break;
            case cudf::type_id::INT64:
                cloned->inner = std::make_unique<cudf::numeric_scalar<int64_t>>(
                    static_cast<cudf::numeric_scalar<int64_t> const&>(*inner));
                break;
            case cudf::type_id::UINT8:
                cloned->inner = std::make_unique<cudf::numeric_scalar<uint8_t>>(
                    static_cast<cudf::numeric_scalar<uint8_t> const&>(*inner));
                break;
            case cudf::type_id::UINT16:
                cloned->inner = std::make_unique<cudf::numeric_scalar<uint16_t>>(
                    static_cast<cudf::numeric_scalar<uint16_t> const&>(*inner));
                break;
            case cudf::type_id::UINT32:
                cloned->inner = std::make_unique<cudf::numeric_scalar<uint32_t>>(
                    static_cast<cudf::numeric_scalar<uint32_t> const&>(*inner));
                break;
            case cudf::type_id::UINT64:
                cloned->inner = std::make_unique<cudf::numeric_scalar<uint64_t>>(
                    static_cast<cudf::numeric_scalar<uint64_t> const&>(*inner));
                break;
            case cudf::type_id::FLOAT32:
                cloned->inner = std::make_unique<cudf::numeric_scalar<float>>(
                    static_cast<cudf::numeric_scalar<float> const&>(*inner));
                break;
            case cudf::type_id::FLOAT64:
                cloned->inner = std::make_unique<cudf::numeric_scalar<double>>(
                    static_cast<cudf::numeric_scalar<double> const&>(*inner));
                break;
            case cudf::type_id::BOOL8:
                cloned->inner = std::make_unique<cudf::numeric_scalar<bool>>(
                    static_cast<cudf::numeric_scalar<bool> const&>(*inner));
                break;
            case cudf::type_id::STRING:
                cloned->inner = std::make_unique<cudf::string_scalar>(
                    static_cast<cudf::string_scalar const&>(*inner));
                break;
            // Add more types as needed
            default:
                throw std::runtime_error("Unsupported scalar type for cloning");
        }

        return cloned;
    }
} // namespace libcudf_bridge
