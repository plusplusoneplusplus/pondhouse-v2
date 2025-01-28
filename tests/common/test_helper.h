#pragma once

#include <gtest/gtest.h>
#include "common/result.h"

// Macro to verify Result<T> objects with better error messages
#define VERIFY_RESULT(result) \
    ASSERT_TRUE((result).ok()) << "Operation failed: " << (result).error()

// Macro to verify Result<T> objects with custom message
#define VERIFY_RESULT_MSG(result, msg) \
    ASSERT_TRUE((result).ok()) << msg << ": " << (result).error()

// Helper macro for verifying that a result fails with a specific error code
#define VERIFY_ERROR_CODE(result, expected_code) \
    ASSERT_FALSE((result).ok()); \
    ASSERT_EQ((result).error().code(), (expected_code)) << "Unexpected error: " << (result).error()
