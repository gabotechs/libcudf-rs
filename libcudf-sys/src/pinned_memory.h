#pragma once

#include <cstddef>
#include <memory>

#include <cudf/utilities/pinned_memory.hpp>

namespace libcudf_bridge {
    // cxx cannot represent std::optional<size_t>, so this wrapper preserves
    // cudf::pinned_mr_options without introducing defaults in the bridge.
    struct PinnedMrOptions {
        cudf::pinned_mr_options inner;

        PinnedMrOptions();
        explicit PinnedMrOptions(std::size_t pool_size);
    };

    [[nodiscard]] std::unique_ptr<PinnedMrOptions> make_pinned_mr_options();

    [[nodiscard]] std::unique_ptr<PinnedMrOptions> make_pinned_mr_options_with_pool_size(
        std::size_t pool_size);

    [[nodiscard]] bool config_default_pinned_memory_resource(const PinnedMrOptions& options);

    void set_allocate_host_as_pinned_threshold(std::size_t threshold_bytes);
}  // namespace libcudf_bridge
