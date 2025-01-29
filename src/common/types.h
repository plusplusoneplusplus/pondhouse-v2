#pragma once

namespace pond::common {

using LSN = uint64_t;
constexpr LSN INVALID_LSN = std::numeric_limits<LSN>::max();
using Timestamp = uint64_t;

}