#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "scalar.h"

#include <cudf/ast/expressions.hpp>

namespace libcudf_bridge {
    struct AstExpressionTree {
        cudf::ast::tree inner;

        AstExpressionTree();

        ~AstExpressionTree();
    };

    std::unique_ptr<AstExpressionTree> ast_expression_tree_create();

    std::size_t ast_expression_tree_add_column_reference(
        AstExpressionTree& tree,
        int32_t column_index,
        int32_t table_reference);

    std::size_t ast_expression_tree_add_literal(
        AstExpressionTree& tree,
        const Scalar& scalar);

    std::size_t ast_expression_tree_add_unary_operation(
        AstExpressionTree& tree,
        int32_t ast_operator,
        std::size_t input_index);

    std::size_t ast_expression_tree_add_operation(
        AstExpressionTree& tree,
        int32_t ast_operator,
        std::size_t left_index,
        std::size_t right_index);
} // namespace libcudf_bridge
