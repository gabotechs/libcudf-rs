#include "io.h"
#include "libcudf-sys/src/lib.rs.h"

#include <cudf/io/parquet.hpp>

namespace libcudf_bridge {
    // Parquet I/O
    std::unique_ptr<Table> read_parquet(rust::Str filename) {
        std::string filename_str(filename.data(), filename.size());
        auto options = cudf::io::parquet_reader_options::builder(cudf::io::source_info{filename_str});
        auto [tbl, metadata] = cudf::io::read_parquet(options.build());

        auto table = std::make_unique<Table>();
        table->inner = std::move(tbl);
        return table;
    }

    void write_parquet(const TableView &table, const rust::Str filename) {
        const std::string filename_str(filename.data(), filename.size());
        auto options = cudf::io::parquet_writer_options::builder(
            cudf::io::sink_info{filename_str},
            *table.inner
        );
        cudf::io::write_parquet(options.build());
    }
} // namespace libcudf_bridge
