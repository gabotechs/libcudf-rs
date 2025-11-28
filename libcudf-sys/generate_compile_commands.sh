#!/bin/bash
# Generate compile_commands.json for clangd

CUDF_ROOT="${CUDF_ROOT:-$HOME/github/cudf}"
CUDA_ROOT="${CUDA_ROOT:-/usr/local/cuda}"

# Find the cxx generated headers
# First try to find the most recent build directory
CXX_BUILD_DIR=$(find ../target/debug/build -type d -name "out" -path "*/libcudf-sys-*/out" 2>/dev/null | sort -r | head -1)
if [ -z "$CXX_BUILD_DIR" ]; then
    echo "Warning: Could not find cxx build output. Run 'cargo build' first."
    CXX_INCLUDE=""
    CXX_CRATE=""
else
    CXX_INCLUDE="-I $CXX_BUILD_DIR/cxxbridge/include"
    CXX_CRATE="-I $CXX_BUILD_DIR/cxxbridge/crate"
fi

cat > compile_commands.json <<EOF
[
  {
    "directory": "$(pwd)",
    "command": "c++ -std=c++20 -I src $CXX_INCLUDE $CXX_CRATE -I $CUDF_ROOT/cpp/include -I $CUDF_ROOT/cpp/build/include -I $CUDF_ROOT/cpp/build/_deps/rmm-src/cpp/include -I $CUDF_ROOT/cpp/build/_deps/cccl-src/libcudacxx/include -I $CUDF_ROOT/cpp/build/_deps/cccl-src/thrust -I $CUDF_ROOT/cpp/build/_deps/cccl-src/cub -I $CUDA_ROOT/include -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -Wno-unused-parameter -Wno-deprecated-declarations -c src/bridge.cpp",
    "file": "src/bridge.cpp"
  },
  {
    "directory": "$(pwd)",
    "command": "c++ -std=c++20 -I src $CXX_INCLUDE $CXX_CRATE -I $CUDF_ROOT/cpp/include -I $CUDF_ROOT/cpp/build/include -I $CUDF_ROOT/cpp/build/_deps/rmm-src/cpp/include -I $CUDF_ROOT/cpp/build/_deps/cccl-src/libcudacxx/include -I $CUDF_ROOT/cpp/build/_deps/cccl-src/thrust -I $CUDF_ROOT/cpp/build/_deps/cccl-src/cub -I $CUDA_ROOT/include -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE -Wno-unused-parameter -Wno-deprecated-declarations -c src/bridge.h",
    "file": "src/bridge.h"
  }
]
EOF

echo "Generated compile_commands.json"
if [ -n "$CXX_BUILD_DIR" ]; then
    echo "Using cxx headers from: $CXX_BUILD_DIR"
fi
