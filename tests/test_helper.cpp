#include "test_helper.h"

namespace pond::test {

std::string GenerateKey(int i, int width) {
    char key_buffer[16];
    snprintf(key_buffer, sizeof(key_buffer), "key%0*d", width, i);
    return std::string(key_buffer);
}

std::vector<std::string> GenerateKeys(int count, int width) {
    std::vector<std::string> keys;
    for (int i = 0; i < count; ++i) {
        keys.push_back(GenerateKey(i, width));
    }
    return keys;
}

}  // namespace pond::test
