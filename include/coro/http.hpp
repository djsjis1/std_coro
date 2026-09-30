#pragma once

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace coro {

    using HttpHeaders = std::vector<std::pair<std::string, std::string>>;

    struct HttpResponse {
        int status = 200;
        std::string version;
        HttpHeaders headers; // Duplicate fields retain their order.
        HttpHeaders trailers;
        std::string body; // Transfer chunks removed; content encoding is not decoded.
        bool keep_alive = true;

        /// First matching field; the returned view borrows this response.
        std::string_view header(std::string_view name) const noexcept {
            for (const auto& [field, value] : headers) {
                if (field.size() != name.size())
                    continue;
                bool equal = true;
                for (std::size_t i = 0; i < name.size(); ++i) {
                    auto lower = [](char c) { return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c; };
                    if (lower(field[i]) != lower(name[i])) {
                        equal = false;
                        break;
                    }
                }
                if (equal)
                    return value;
            }
            return {};
        }
    };

    class HttpError : public std::runtime_error {
      public:
        using std::runtime_error::runtime_error;
    };

} // namespace coro
