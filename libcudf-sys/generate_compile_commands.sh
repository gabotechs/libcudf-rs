#!/bin/bash
# Generate compile_commands.json for clangd
#
# Usage:
#   From project root: ./libcudf-sys/generate_compile_commands.sh
#   Or from anywhere:  /path/to/libcudf-rs/libcudf-sys/generate_compile_commands.sh
#
# This generates compile_commands.json at the project root, which allows
# C++ language servers (like clangd) to provide IDE features for all C++ files.
#
# Note: Run 'cargo build' first to generate the cxx bridge headers.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find the cxx generated headers
CXX_BUILD_DIR=$(find "$PROJECT_ROOT/target/debug/build" -type d -name "out" -path "*/libcudf-sys-*/out" 2>/dev/null | sort -r | head -1)
if [ -z "$CXX_BUILD_DIR" ]; then
    echo "Error: Could not find cxx build output. Run 'cargo build' first."
    exit 1
fi

# Detect prebuilt directory (should be in the OUT_DIR)
PREBUILT_DIR="$CXX_BUILD_DIR/prebuilt"
if [ ! -d "$PREBUILT_DIR" ]; then
    echo "Error: Prebuilt directory not found at $PREBUILT_DIR"
    echo "Run 'cargo build' first to download prebuilt libraries."
    exit 1
fi

# Detect cuDF source headers
CUDF_SRC_DIR=$(find "$CXX_BUILD_DIR" -maxdepth 1 -type d -name "cudf-*" 2>/dev/null | head -1)
if [ -z "$CUDF_SRC_DIR" ]; then
    echo "Error: cuDF source headers not found. Run 'cargo build' first."
    exit 1
fi

# Detect nanoarrow
NANOARROW_DIR="$CXX_BUILD_DIR/arrow-nanoarrow"
if [ ! -d "$NANOARROW_DIR" ]; then
    echo "Error: Nanoarrow headers not found. Run 'cargo build' first."
    exit 1
fi

CUDA_ROOT="${CUDA_ROOT:-/usr/local/cuda}"

# Build the include paths
INCLUDES=""
INCLUDES="$INCLUDES -I $CXX_BUILD_DIR/cxxbridge/include"
INCLUDES="$INCLUDES -I $CXX_BUILD_DIR/cxxbridge/crate"
INCLUDES="$INCLUDES -I libcudf-sys/src"
INCLUDES="$INCLUDES -I $PREBUILT_DIR/libcudf/include"
INCLUDES="$INCLUDES -I $CUDF_SRC_DIR/cpp/include"
INCLUDES="$INCLUDES -I $PREBUILT_DIR/libcudf/include/rapids"
INCLUDES="$INCLUDES -I $PREBUILT_DIR/librmm/include"
INCLUDES="$INCLUDES -I $PREBUILT_DIR/librmm/include/rapids"
INCLUDES="$INCLUDES -I $PREBUILT_DIR/libkvikio/include"
INCLUDES="$INCLUDES -I $NANOARROW_DIR/src"
INCLUDES="$INCLUDES -I $CUDA_ROOT/include"

DEFINES="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"
WARNINGS="-Wno-unused-parameter -Wno-deprecated-declarations"

# Array of C++ source files
CPP_FILES=(
    "libcudf-sys/src/table.cpp"
    "libcudf-sys/src/column.cpp"
    "libcudf-sys/src/groupby.cpp"
    "libcudf-sys/src/aggregation.cpp"
    "libcudf-sys/src/io.cpp"
    "libcudf-sys/src/operations.cpp"
    "libcudf-sys/src/binaryop.cpp"
    "libcudf-sys/src/sorting.cpp"
    "libcudf-sys/src/data_type.cpp"
    "libcudf-sys/src/scalar.cpp"
)

# Array of header files
HEADER_FILES=(
    "libcudf-sys/src/table.h"
    "libcudf-sys/src/column.h"
    "libcudf-sys/src/groupby.h"
    "libcudf-sys/src/aggregation.h"
    "libcudf-sys/src/io.h"
    "libcudf-sys/src/operations.h"
    "libcudf-sys/src/binaryop.h"
    "libcudf-sys/src/sorting.h"
    "libcudf-sys/src/data_type.h"
    "libcudf-sys/src/scalar.h"
)

# Start JSON array
cat > "$PROJECT_ROOT/compile_commands.json" << 'EOF_START'
[
EOF_START

# Add entries for C++ files
FIRST=true
for file in "${CPP_FILES[@]}"; do
    if [ "$FIRST" = false ]; then
        echo "," >> "$PROJECT_ROOT/compile_commands.json"
    fi
    FIRST=false

    cat >> "$PROJECT_ROOT/compile_commands.json" <<EOF
  {
    "directory": "$PROJECT_ROOT",
    "command": "c++ -xc++ -std=c++20 $INCLUDES $DEFINES $WARNINGS -c $file",
    "file": "$file"
  }
EOF
done

# Add entries for header files
for file in "${HEADER_FILES[@]}"; do
    echo "," >> "$PROJECT_ROOT/compile_commands.json"
    cat >> "$PROJECT_ROOT/compile_commands.json" <<EOF
  {
    "directory": "$PROJECT_ROOT",
    "command": "c++ -xc++ -std=c++20 $INCLUDES $DEFINES $WARNINGS -c $file",
    "file": "$file"
  }
EOF
done

# Close JSON array
cat >> "$PROJECT_ROOT/compile_commands.json" << 'EOF_END'
]
EOF_END

echo "Generated compile_commands.json in project root"
echo "Using cxx headers from: $CXX_BUILD_DIR"
echo "Using prebuilt libraries from: $PREBUILT_DIR"
echo "Using cuDF source headers from: $CUDF_SRC_DIR"
echo "Using nanoarrow headers from: $NANOARROW_DIR"
