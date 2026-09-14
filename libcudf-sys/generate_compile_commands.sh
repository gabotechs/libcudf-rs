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

# Read the pinned cuDF version so we can select a build directory that matches
# it. Old upgrades leave several libcudf-sys-*/out trees behind, and picking the
# wrong one points the IDE at stale headers.
CUDF_VERSION=$(sed -n 's/^const CUDF_VERSION: &str = "\(.*\)";$/\1/p' "$SCRIPT_DIR/build.rs")
if [ -z "$CUDF_VERSION" ]; then
    echo "Error: could not read CUDF_VERSION from $SCRIPT_DIR/build.rs"
    exit 1
fi

# Prefer the most recently modified build directory that is fully populated for
# the pinned version, rather than simply the newest one.
CXX_BUILD_DIR=""
while read -r _ candidate; do
    if [ -d "$candidate/cudf-$CUDF_VERSION" ] && [ -d "$candidate/librmm/include" ] &&
        [ -d "$candidate/libcudf/include" ] && [ -d "$candidate/cxxbridge/include" ]; then
        CXX_BUILD_DIR="$candidate"
        break
    fi
done < <(find "$PROJECT_ROOT/target/debug/build" -type d -name "out" -path "*/libcudf-sys-*/out" 2>/dev/null -printf '%T@ %p\n' | sort -rn)

if [ -z "$CXX_BUILD_DIR" ]; then
    echo "Error: no build output found for cuDF $CUDF_VERSION. Run 'cargo build' first."
    exit 1
fi

# Prebuilt libraries are now directly in OUT_DIR
LIBCUDF_DIR="$CXX_BUILD_DIR/libcudf"
if [ ! -d "$LIBCUDF_DIR" ]; then
    echo "Error: libcudf directory not found at $LIBCUDF_DIR"
    echo "Run 'cargo build' first to download prebuilt libraries."
    exit 1
fi

LIBRMM_DIR="$CXX_BUILD_DIR/librmm"
if [ ! -d "$LIBRMM_DIR" ]; then
    echo "Error: librmm directory not found at $LIBRMM_DIR"
    echo "Run 'cargo build' first to download prebuilt libraries."
    exit 1
fi

LIBKVIKIO_DIR="$CXX_BUILD_DIR/libkvikio"
if [ ! -d "$LIBKVIKIO_DIR" ]; then
    echo "Error: libkvikio directory not found at $LIBKVIKIO_DIR"
    echo "Run 'cargo build' first to download prebuilt libraries."
    exit 1
fi

# cuDF source headers for the pinned version (validated when selecting the
# build directory above).
CUDF_SRC_DIR="$CXX_BUILD_DIR/cudf-$CUDF_VERSION"

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
INCLUDES="$INCLUDES -I $LIBCUDF_DIR/include"
INCLUDES="$INCLUDES -I $CUDF_SRC_DIR/cpp/include"
INCLUDES="$INCLUDES -I $LIBCUDF_DIR/include/rapids"
INCLUDES="$INCLUDES -I $LIBRMM_DIR/include"
INCLUDES="$INCLUDES -I $LIBRMM_DIR/include/rapids"
INCLUDES="$INCLUDES -I $LIBKVIKIO_DIR/include"
INCLUDES="$INCLUDES -I $CXX_BUILD_DIR/rapids_logger/include"
INCLUDES="$INCLUDES -I $NANOARROW_DIR/src"
INCLUDES="$INCLUDES -I $CUDA_ROOT/include"

DEFINES="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"
WARNINGS="-Wno-unused-parameter -Wno-deprecated-declarations"

mapfile -t CPP_FILES < <(
    find "$PROJECT_ROOT/libcudf-sys/src" -maxdepth 1 -type f -name '*.cpp' \
        -printf 'libcudf-sys/src/%f\n' | sort
)
mapfile -t HEADER_FILES < <(
    find "$PROJECT_ROOT/libcudf-sys/src" -maxdepth 1 -type f -name '*.h' \
        -printf 'libcudf-sys/src/%f\n' | sort
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
echo "Using libcudf from: $LIBCUDF_DIR"
echo "Using librmm from: $LIBRMM_DIR"
echo "Using libkvikio from: $LIBKVIKIO_DIR"
echo "Using cuDF source headers from: $CUDF_SRC_DIR"
echo "Using nanoarrow headers from: $NANOARROW_DIR"
