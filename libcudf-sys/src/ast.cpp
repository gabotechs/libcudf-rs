#include "ast.h"

#include <cudf/fixed_point/fixed_point.hpp>
#include <cudf/scalar/scalar.hpp>
#include <stdexcept>

namespace libcudf_bridge {

static_assert(static_cast<int32_t>(cudf::ast::table_reference::LEFT) == 0);
static_assert(static_cast<int32_t>(cudf::ast::table_reference::RIGHT) == 1);
static_assert(static_cast<int32_t>(cudf::ast::table_reference::OUTPUT) == 2);

#define LIBCUDF_ASSERT_AST_OPERATOR(name, value) \
    static_assert(static_cast<int32_t>(cudf::ast::ast_operator::name) == value)

LIBCUDF_ASSERT_AST_OPERATOR(ADD, 0);
LIBCUDF_ASSERT_AST_OPERATOR(SUB, 1);
LIBCUDF_ASSERT_AST_OPERATOR(MUL, 2);
LIBCUDF_ASSERT_AST_OPERATOR(DIV, 3);
LIBCUDF_ASSERT_AST_OPERATOR(TRUE_DIV, 4);
LIBCUDF_ASSERT_AST_OPERATOR(FLOOR_DIV, 5);
LIBCUDF_ASSERT_AST_OPERATOR(MOD, 6);
LIBCUDF_ASSERT_AST_OPERATOR(PYMOD, 7);
LIBCUDF_ASSERT_AST_OPERATOR(POW, 8);
LIBCUDF_ASSERT_AST_OPERATOR(EQUAL, 9);
LIBCUDF_ASSERT_AST_OPERATOR(NULL_EQUAL, 10);
LIBCUDF_ASSERT_AST_OPERATOR(NOT_EQUAL, 11);
LIBCUDF_ASSERT_AST_OPERATOR(LESS, 12);
LIBCUDF_ASSERT_AST_OPERATOR(GREATER, 13);
LIBCUDF_ASSERT_AST_OPERATOR(LESS_EQUAL, 14);
LIBCUDF_ASSERT_AST_OPERATOR(GREATER_EQUAL, 15);
LIBCUDF_ASSERT_AST_OPERATOR(BITWISE_AND, 16);
LIBCUDF_ASSERT_AST_OPERATOR(BITWISE_OR, 17);
LIBCUDF_ASSERT_AST_OPERATOR(BITWISE_XOR, 18);
LIBCUDF_ASSERT_AST_OPERATOR(LOGICAL_AND, 19);
LIBCUDF_ASSERT_AST_OPERATOR(NULL_LOGICAL_AND, 20);
LIBCUDF_ASSERT_AST_OPERATOR(LOGICAL_OR, 21);
LIBCUDF_ASSERT_AST_OPERATOR(NULL_LOGICAL_OR, 22);
LIBCUDF_ASSERT_AST_OPERATOR(IDENTITY, 23);
LIBCUDF_ASSERT_AST_OPERATOR(IS_NULL, 24);
LIBCUDF_ASSERT_AST_OPERATOR(SIN, 25);
LIBCUDF_ASSERT_AST_OPERATOR(COS, 26);
LIBCUDF_ASSERT_AST_OPERATOR(TAN, 27);
LIBCUDF_ASSERT_AST_OPERATOR(ARCSIN, 28);
LIBCUDF_ASSERT_AST_OPERATOR(ARCCOS, 29);
LIBCUDF_ASSERT_AST_OPERATOR(ARCTAN, 30);
LIBCUDF_ASSERT_AST_OPERATOR(SINH, 31);
LIBCUDF_ASSERT_AST_OPERATOR(COSH, 32);
LIBCUDF_ASSERT_AST_OPERATOR(TANH, 33);
LIBCUDF_ASSERT_AST_OPERATOR(ARCSINH, 34);
LIBCUDF_ASSERT_AST_OPERATOR(ARCCOSH, 35);
LIBCUDF_ASSERT_AST_OPERATOR(ARCTANH, 36);
LIBCUDF_ASSERT_AST_OPERATOR(EXP, 37);
LIBCUDF_ASSERT_AST_OPERATOR(LOG, 38);
LIBCUDF_ASSERT_AST_OPERATOR(SQRT, 39);
LIBCUDF_ASSERT_AST_OPERATOR(CBRT, 40);
LIBCUDF_ASSERT_AST_OPERATOR(CEIL, 41);
LIBCUDF_ASSERT_AST_OPERATOR(FLOOR, 42);
LIBCUDF_ASSERT_AST_OPERATOR(ABS, 43);
LIBCUDF_ASSERT_AST_OPERATOR(RINT, 44);
LIBCUDF_ASSERT_AST_OPERATOR(BIT_INVERT, 45);
LIBCUDF_ASSERT_AST_OPERATOR(NOT, 46);
LIBCUDF_ASSERT_AST_OPERATOR(CAST_TO_INT64, 47);
LIBCUDF_ASSERT_AST_OPERATOR(CAST_TO_UINT64, 48);
LIBCUDF_ASSERT_AST_OPERATOR(CAST_TO_FLOAT64, 49);

#undef LIBCUDF_ASSERT_AST_OPERATOR

namespace {
template <typename ScalarType>
std::size_t add_literal(
    AstExpressionTree& tree,
    const Scalar& scalar,
    const cudf::type_id expected_type)
{
    if (!scalar.inner) {
        throw std::runtime_error("Cannot add null scalar to AST expression tree");
    }
    if (scalar.inner->type().id() != expected_type) {
        throw std::invalid_argument("Scalar type does not match AST literal constructor");
    }
    // cuDF's literal constructors take non-const references but retain only const
    // references. The bridge keeps the wrapper const and borrows its stable allocation.
    tree.inner.emplace<cudf::ast::literal>(static_cast<ScalarType&>(*scalar.inner));
    return tree.inner.size() - 1;
}
} // namespace

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

#define LIBCUDF_DEFINE_AST_LITERAL(name, scalar_type, expected_id)                 \
    std::size_t ast_expression_tree_add_literal_##name(                            \
        AstExpressionTree& tree, const Scalar& scalar)                             \
    {                                                                               \
        return add_literal<scalar_type>(tree, scalar, cudf::type_id::expected_id);  \
    }

LIBCUDF_DEFINE_AST_LITERAL(int8, cudf::numeric_scalar<int8_t>, INT8)
LIBCUDF_DEFINE_AST_LITERAL(int16, cudf::numeric_scalar<int16_t>, INT16)
LIBCUDF_DEFINE_AST_LITERAL(int32, cudf::numeric_scalar<int32_t>, INT32)
LIBCUDF_DEFINE_AST_LITERAL(int64, cudf::numeric_scalar<int64_t>, INT64)
LIBCUDF_DEFINE_AST_LITERAL(uint8, cudf::numeric_scalar<uint8_t>, UINT8)
LIBCUDF_DEFINE_AST_LITERAL(uint16, cudf::numeric_scalar<uint16_t>, UINT16)
LIBCUDF_DEFINE_AST_LITERAL(uint32, cudf::numeric_scalar<uint32_t>, UINT32)
LIBCUDF_DEFINE_AST_LITERAL(uint64, cudf::numeric_scalar<uint64_t>, UINT64)
LIBCUDF_DEFINE_AST_LITERAL(float32, cudf::numeric_scalar<float>, FLOAT32)
LIBCUDF_DEFINE_AST_LITERAL(float64, cudf::numeric_scalar<double>, FLOAT64)
LIBCUDF_DEFINE_AST_LITERAL(bool8, cudf::numeric_scalar<bool>, BOOL8)
LIBCUDF_DEFINE_AST_LITERAL(string, cudf::string_scalar, STRING)
LIBCUDF_DEFINE_AST_LITERAL(timestamp_days, cudf::timestamp_scalar<cudf::timestamp_D>, TIMESTAMP_DAYS)
LIBCUDF_DEFINE_AST_LITERAL(timestamp_seconds, cudf::timestamp_scalar<cudf::timestamp_s>, TIMESTAMP_SECONDS)
LIBCUDF_DEFINE_AST_LITERAL(timestamp_milliseconds, cudf::timestamp_scalar<cudf::timestamp_ms>, TIMESTAMP_MILLISECONDS)
LIBCUDF_DEFINE_AST_LITERAL(timestamp_microseconds, cudf::timestamp_scalar<cudf::timestamp_us>, TIMESTAMP_MICROSECONDS)
LIBCUDF_DEFINE_AST_LITERAL(timestamp_nanoseconds, cudf::timestamp_scalar<cudf::timestamp_ns>, TIMESTAMP_NANOSECONDS)
LIBCUDF_DEFINE_AST_LITERAL(duration_days, cudf::duration_scalar<cudf::duration_D>, DURATION_DAYS)
LIBCUDF_DEFINE_AST_LITERAL(duration_seconds, cudf::duration_scalar<cudf::duration_s>, DURATION_SECONDS)
LIBCUDF_DEFINE_AST_LITERAL(duration_milliseconds, cudf::duration_scalar<cudf::duration_ms>, DURATION_MILLISECONDS)
LIBCUDF_DEFINE_AST_LITERAL(duration_microseconds, cudf::duration_scalar<cudf::duration_us>, DURATION_MICROSECONDS)
LIBCUDF_DEFINE_AST_LITERAL(duration_nanoseconds, cudf::duration_scalar<cudf::duration_ns>, DURATION_NANOSECONDS)
LIBCUDF_DEFINE_AST_LITERAL(decimal32, cudf::fixed_point_scalar<numeric::decimal32>, DECIMAL32)
LIBCUDF_DEFINE_AST_LITERAL(decimal64, cudf::fixed_point_scalar<numeric::decimal64>, DECIMAL64)
LIBCUDF_DEFINE_AST_LITERAL(decimal128, cudf::fixed_point_scalar<numeric::decimal128>, DECIMAL128)

#undef LIBCUDF_DEFINE_AST_LITERAL

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
