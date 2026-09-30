#pragma once

#include <cstddef>

namespace coro {

    /// Result of cooperative shutdown. An unfinished task is still owned and
    /// must be allowed to finish before its event loop and resources are destroyed.
    struct ShutdownReport {
        bool drained = false;
        std::size_t unfinished = 0;
    };

} // namespace coro
