#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "scalar.h"
#include "rust/cxx.h"

#include <cudf/ast/expressions.hpp>

namespace libcudf_bridge {
    struct AstExpressionTree {
        cudf::ast::tree inner;

        AstExpressionTree();

        ~AstExpressionTree();
    };

    [[nodiscard]] std::unique_ptr<AstExpressionTree> ast_expression_tree_create();

    [[nodiscard]] std::size_t ast_expression_tree_add_column_reference(
        AstExpressionTree& tree,
        int32_t column_index,
        int32_t table_reference);

    [[nodiscard]] std::size_t ast_expression_tree_add_column_name_reference(
        AstExpressionTree& tree,
        rust::Str column_name);

#define LIBCUDF_DECLARE_AST_LITERAL(name) \
    [[nodiscard]] std::size_t ast_expression_tree_add_literal_##name( \
        AstExpressionTree& tree, const Scalar& scalar)

    LIBCUDF_DECLARE_AST_LITERAL(int8);
    LIBCUDF_DECLARE_AST_LITERAL(int16);
    LIBCUDF_DECLARE_AST_LITERAL(int32);
    LIBCUDF_DECLARE_AST_LITERAL(int64);
    LIBCUDF_DECLARE_AST_LITERAL(uint8);
    LIBCUDF_DECLARE_AST_LITERAL(uint16);
    LIBCUDF_DECLARE_AST_LITERAL(uint32);
    LIBCUDF_DECLARE_AST_LITERAL(uint64);
    LIBCUDF_DECLARE_AST_LITERAL(float32);
    LIBCUDF_DECLARE_AST_LITERAL(float64);
    LIBCUDF_DECLARE_AST_LITERAL(bool8);
    LIBCUDF_DECLARE_AST_LITERAL(string);
    LIBCUDF_DECLARE_AST_LITERAL(timestamp_days);
    LIBCUDF_DECLARE_AST_LITERAL(timestamp_seconds);
    LIBCUDF_DECLARE_AST_LITERAL(timestamp_milliseconds);
    LIBCUDF_DECLARE_AST_LITERAL(timestamp_microseconds);
    LIBCUDF_DECLARE_AST_LITERAL(timestamp_nanoseconds);
    LIBCUDF_DECLARE_AST_LITERAL(duration_days);
    LIBCUDF_DECLARE_AST_LITERAL(duration_seconds);
    LIBCUDF_DECLARE_AST_LITERAL(duration_milliseconds);
    LIBCUDF_DECLARE_AST_LITERAL(duration_microseconds);
    LIBCUDF_DECLARE_AST_LITERAL(duration_nanoseconds);
    LIBCUDF_DECLARE_AST_LITERAL(decimal32);
    LIBCUDF_DECLARE_AST_LITERAL(decimal64);
    LIBCUDF_DECLARE_AST_LITERAL(decimal128);

#undef LIBCUDF_DECLARE_AST_LITERAL

    [[nodiscard]] std::size_t ast_expression_tree_add_unary_operation(
        AstExpressionTree& tree,
        int32_t ast_operator,
        std::size_t input_index);

    [[nodiscard]] std::size_t ast_expression_tree_add_operation(
        AstExpressionTree& tree,
        int32_t ast_operator,
        std::size_t left_index,
        std::size_t right_index);
} // namespace libcudf_bridge
