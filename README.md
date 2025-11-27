# libcudf-rs

Rust bindings for [libcudf](https://docs.rapids.ai/api/libcudf/stable/), the GPU-accelerated DataFrame library from RAPIDS.

## Overview

This project provides safe, idiomatic Rust bindings to cuDF using the [cxx](https://cxx.rs/) library for seamless C++/Rust interoperability. cuDF enables GPU-accelerated operations on DataFrames, offering significant performance improvements for data processing tasks.

## Project Structure

The project is organized as a Rust workspace with two crates:

- **libcudf-sys**: Low-level FFI bindings to libcudf using cxx
- **libcudf-rs**: Safe, high-level Rust API wrapping the FFI bindings

## Prerequisites

Before building this project, you need:

1. **CUDA Toolkit**: Required for GPU operations
   - Install from [NVIDIA CUDA Downloads](https://developer.nvidia.com/cuda-downloads)

2. **libcudf**: The cuDF C++ library
   - Build from source: [cuDF build instructions](https://github.com/rapidsai/cudf)
   - Or install via conda: `conda install -c rapidsai -c conda-forge cudf`

3. **Rust toolchain**:
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

4. **C++ compiler**: GCC 9+ or Clang that supports C++17

## Building

Once dependencies are installed:

```bash
# Build the project
cargo build

# Run tests (requires CUDA-capable GPU)
cargo test

# Build with release optimizations
cargo build --release
```

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
libcudf-rs = { path = "path/to/libcudf-rs" }
```

### Example

```rust
use libcudf_rs::Table;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read a CSV file into a GPU-accelerated table
    let table = Table::from_csv("data.csv")?;

    println!("Loaded {} rows and {} columns",
             table.num_rows(), table.num_columns());

    // Write the table back to CSV
    table.to_csv("output.csv")?;

    Ok(())
}
```

### Running Examples

```bash
# Run the basic usage example
cargo run --example basic_usage
```

## Architecture

This project uses the `cxx` crate to provide safe interoperability between Rust and C++:

1. **C++ Bridge Layer** (`libcudf-sys/src/bridge.{h,cpp}`):
   - Wraps cuDF C++ APIs in cxx-compatible interfaces
   - Handles lifetime management and error conversion

2. **Rust FFI Layer** (`libcudf-sys/src/lib.rs`):
   - Defines the Rust side of the FFI boundary using `#[cxx::bridge]`
   - Exposes unsafe bindings to C++ functions

3. **Safe Rust API** (`src/lib.rs`):
   - Provides idiomatic, safe Rust wrappers
   - Handles error conversion and resource management

## Current Features

- Create empty tables
- Read CSV files into GPU memory
- Write tables to CSV files
- Query table dimensions (rows, columns)

## Extending the Bindings

To add new cuDF functionality:

1. Add C++ wrapper functions in `libcudf-sys/src/bridge.{h,cpp}`
2. Declare the interface in the `#[cxx::bridge]` macro in `libcudf-sys/src/lib.rs`
3. Add safe Rust wrappers in `src/lib.rs`
4. Rebuild with `cargo build`

Example of adding a new function:

```cpp
// In bridge.h
size_t count_nulls(const Table& table);

// In bridge.cpp
size_t count_nulls(const Table& table) {
    // Implementation using cuDF APIs
}
```

```rust
// In libcudf-sys/src/lib.rs, inside #[cxx::bridge]
unsafe extern "C++" {
    fn count_nulls(table: &Table) -> usize;
}

// In src/lib.rs
impl Table {
    pub fn count_nulls(&self) -> usize {
        ffi::count_nulls(&self.inner)
    }
}
```

## Limitations

This is a foundational scaffold. Current limitations:

- Limited API coverage (only basic CSV I/O and table operations)
- No column-level operations yet
- No data type introspection
- No support for other file formats (Parquet, ORC, etc.)
- Requires libcudf to be installed system-wide

## Contributing

Contributions are welcome! Areas for improvement:

- Expand API coverage for more cuDF operations
- Add support for different data types and column operations
- Implement DataFrame-style operations (filter, join, groupby, etc.)
- Add benchmarks comparing performance with CPU-based solutions
- Improve error handling and ergonomics

## License

This project's license should match your requirements. cuDF itself is Apache 2.0 licensed.

## Resources

- [libcudf Documentation](https://docs.rapids.ai/api/libcudf/stable/)
- [cxx Documentation](https://cxx.rs/)
- [RAPIDS cuDF Python API](https://docs.rapids.ai/api/cudf/stable/)
- [cuDF GitHub Repository](https://github.com/rapidsai/cudf)

## Authors

- notfilippo
- gabotechs
