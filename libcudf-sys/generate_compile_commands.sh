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

# Detect the cuDF build directory
if [ -d "$PROJECT_ROOT/.cudf-build" ]; then
    CUDF_BUILD=$(find "$PROJECT_ROOT/.cudf-build" -maxdepth 1 -type d -name "cudf-*" | sort -r | head -1)
    if [ -n "$CUDF_BUILD" ]; then
        CUDF_ROOT="$CUDF_BUILD"
    else
        CUDF_ROOT="${CUDF_ROOT:-$HOME/github/cudf}"
    fi
else
    CUDF_ROOT="${CUDF_ROOT:-$HOME/github/cudf}"
fi

CUDA_ROOT="${CUDA_ROOT:-/usr/local/cuda}"

# Find the cxx generated headers
CXX_BUILD_DIR=$(find "$PROJECT_ROOT/target/debug/build" -type d -name "out" -path "*/libcudf-sys-*/out" 2>/dev/null | sort -r | head -1)
if [ -z "$CXX_BUILD_DIR" ]; then
    echo "Warning: Could not find cxx build output. Run 'cargo build' first."
    CXX_INCLUDE=""
    CXX_CRATE=""
else
    CXX_INCLUDE="-I $CXX_BUILD_DIR/cxxbridge/include"
    CXX_CRATE="-I $CXX_BUILD_DIR/cxxbridge/crate"
fi

# Build the include paths
INCLUDES="$CXX_INCLUDE $CXX_CRATE"
INCLUDES="$INCLUDES -I libcudf-sys/src"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/include"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/include"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/rmm-src/include"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/cccl-src/libcudacxx/include"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/cccl-src/thrust"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/cccl-src/cub"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/nanoarrow-src/src"
INCLUDES="$INCLUDES -I $CUDF_ROOT/cpp/build/build/_deps/nanoarrow-build/src"
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
)

# Array of header files
HEADER_FILES=(
    "libcudf-sys/src/bridge.h"
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
    "command": "c++ -std=c++20 $INCLUDES $DEFINES $WARNINGS -c $file",
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
    "command": "c++ -std=c++20 $INCLUDES $DEFINES $WARNINGS -c $file",
    "file": "$file"
  }
EOF
done

# Close JSON array
cat >> "$PROJECT_ROOT/compile_commands.json" << 'EOF_END'
]
EOF_END

echo "Generated compile_commands.json in project root"
if [ -n "$CXX_BUILD_DIR" ]; then
    echo "Using cxx headers from: $CXX_BUILD_DIR"
fi
echo "Using cuDF from: $CUDF_ROOT"
