#include "common/error.h"

#include <sstream>

namespace pond::common {

std::string Error::to_string() const {
    std::ostringstream ss;
    ss << "Error: " << static_cast<int>(code_) << " " << message_;
    return ss.str();
}

}  // namespace pond::common
