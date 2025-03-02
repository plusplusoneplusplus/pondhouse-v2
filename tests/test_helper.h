#pragma once

#include <gtest/gtest.h>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/time.h"

// Macro to verify Result<T> objects with better error messages
#define VERIFY_RESULT(result) ASSERT_TRUE((result).ok()) << "Operation failed: " << (result).error().to_string()

// Macro to verify Result<T> objects with custom message
#define VERIFY_RESULT_MSG(result, msg) ASSERT_TRUE((result).ok()) << msg << ": " << (result).error().to_string()

// Helper macro for verifying that a result fails with a specific error code
#define VERIFY_ERROR_CODE(result, expected_code) \
    ASSERT_FALSE((result).ok());                 \
    ASSERT_EQ((result).error().code(), (expected_code)) << "Unexpected error: " << (result).error().to_string()

namespace pond::test {

struct TestKvEntry {
    std::string key;
    common::HybridTime version;
    common::DataChunk value;
};

std::string GenerateKey(int i, int width = 3);

std::vector<std::string> GenerateKeys(int count, int width = 3);

TestKvEntry GenerateTestKvEntry(const std::string& key, common::HybridTime version, const std::string& value);
}  // namespace pond::test