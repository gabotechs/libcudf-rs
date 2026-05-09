#include "ast.h"

#include <cudf/fixed_point/fixed_point.hpp>
#include <cudf/scalar/scalar.hpp>
#include <stdexcept>

namespace libcudf_bridge {

AstExpressionTree::AstExpressionTree() = default;

AstExpressionTree::~AstExpressionTree() = default;

std::unique_ptr<AstExpressionTree> ast_expression_tree_create()
{
    return std::make_unique<AstExpressionTree>();
}

std::size_t ast_expression_tree_add_column_reference(
    AstExpressionTree& tree,
    int32_t column_index,
    int32_t table_reference)
{
    tree.inner.emplace<cudf::ast::column_reference>(
        column_index,
        static_cast<cudf::ast::table_reference>(table_reference));
    return tree.inner.size() - 1;
}

std::size_t ast_expression_tree_add_column_name_reference(
    AstExpressionTree& tree,
    rust::Str column_name)
{
    tree.inner.emplace<cudf::ast::column_name_reference>(
        std::string(column_name.data(), column_name.size()));
    return tree.inner.size() - 1;
}

std::size_t ast_expression_tree_add_literal(AstExpressionTree& tree, const Scalar& scalar)
{
    if (!scalar.inner) {
        throw std::runtime_error("Cannot add null scalar to AST expression tree");
    }

    switch (scalar.inner->type().id()) {
        case cudf::type_id::INT8:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<int8_t>&>(*scalar.inner));
            break;
        case cudf::type_id::INT16:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<int16_t>&>(*scalar.inner));
            break;
        case cudf::type_id::INT32:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<int32_t>&>(*scalar.inner));
            break;
        case cudf::type_id::INT64:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<int64_t>&>(*scalar.inner));
            break;
        case cudf::type_id::UINT8:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<uint8_t>&>(*scalar.inner));
            break;
        case cudf::type_id::UINT16:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<uint16_t>&>(*scalar.inner));
            break;
        case cudf::type_id::UINT32:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<uint32_t>&>(*scalar.inner));
            break;
        case cudf::type_id::UINT64:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<uint64_t>&>(*scalar.inner));
            break;
        case cudf::type_id::FLOAT32:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<float>&>(*scalar.inner));
            break;
        case cudf::type_id::FLOAT64:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<double>&>(*scalar.inner));
            break;
        case cudf::type_id::BOOL8:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::numeric_scalar<bool>&>(*scalar.inner));
            break;
        case cudf::type_id::STRING:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::string_scalar&>(*scalar.inner));
            break;
        case cudf::type_id::TIMESTAMP_DAYS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::timestamp_scalar<cudf::timestamp_D>&>(*scalar.inner));
            break;
        case cudf::type_id::TIMESTAMP_SECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::timestamp_scalar<cudf::timestamp_s>&>(*scalar.inner));
            break;
        case cudf::type_id::TIMESTAMP_MILLISECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::timestamp_scalar<cudf::timestamp_ms>&>(*scalar.inner));
            break;
        case cudf::type_id::TIMESTAMP_MICROSECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::timestamp_scalar<cudf::timestamp_us>&>(*scalar.inner));
            break;
        case cudf::type_id::TIMESTAMP_NANOSECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::timestamp_scalar<cudf::timestamp_ns>&>(*scalar.inner));
            break;
        case cudf::type_id::DURATION_DAYS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::duration_scalar<cudf::duration_D>&>(*scalar.inner));
            break;
        case cudf::type_id::DURATION_SECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::duration_scalar<cudf::duration_s>&>(*scalar.inner));
            break;
        case cudf::type_id::DURATION_MILLISECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::duration_scalar<cudf::duration_ms>&>(*scalar.inner));
            break;
        case cudf::type_id::DURATION_MICROSECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::duration_scalar<cudf::duration_us>&>(*scalar.inner));
            break;
        case cudf::type_id::DURATION_NANOSECONDS:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::duration_scalar<cudf::duration_ns>&>(*scalar.inner));
            break;
        case cudf::type_id::DECIMAL32:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::fixed_point_scalar<numeric::decimal32>&>(*scalar.inner));
            break;
        case cudf::type_id::DECIMAL64:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::fixed_point_scalar<numeric::decimal64>&>(*scalar.inner));
            break;
        case cudf::type_id::DECIMAL128:
            tree.inner.emplace<cudf::ast::literal>(
                static_cast<cudf::fixed_point_scalar<numeric::decimal128>&>(*scalar.inner));
            break;
        default:
            throw std::runtime_error("Unsupported scalar type for AST literal");
    }

    return tree.inner.size() - 1;
}

std::size_t ast_expression_tree_add_operation(
    AstExpressionTree& tree,
    int32_t ast_operator,
    std::size_t left_index,
    std::size_t right_index)
{
    tree.inner.emplace<cudf::ast::operation>(
        static_cast<cudf::ast::ast_operator>(ast_operator),
        tree.inner.at(left_index),
        tree.inner.at(right_index));
    return tree.inner.size() - 1;
}

std::size_t ast_expression_tree_add_unary_operation(
    AstExpressionTree& tree,
    int32_t ast_operator,
    std::size_t input_index)
{
    tree.inner.emplace<cudf::ast::operation>(
        static_cast<cudf::ast::ast_operator>(ast_operator),
        tree.inner.at(input_index));
    return tree.inner.size() - 1;
}

} // namespace libcudf_bridge
