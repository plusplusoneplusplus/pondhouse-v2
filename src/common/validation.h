#pragma once

#include <regex>
#include <string>

namespace pond::common {

// verify that db and table name is valid
// the name must be alphanumeric and contain only underscores, dots, or hyphens
inline bool ValidateName(const std::string& name) {
    return !name.empty() && std::regex_match(name, std::regex("^[a-zA-Z0-9_.-]+$"));
}

}  // namespace pond::common
