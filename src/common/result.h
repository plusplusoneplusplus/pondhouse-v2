#pragma once

#include <concepts>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "error.h"

#define RETURN_IF_ERROR(expr)        \
    do {                             \
        auto result_tmp = (expr);    \
        if (result_tmp.hasError()) { \
            return result_tmp;       \
        }                            \
    } while (false)

#define RETURN_IF_ERROR_T(T, expr)                                                      \
    do {                                                                                \
        auto result_tmp = (expr);                                                       \
        if (result_tmp.hasError()) {                                                    \
            LOG_ERROR("Failed to %s: %s", #expr, result_tmp.error().message().c_str()); \
            return pond::common::as_result_type_t<T>::failure(result_tmp.error());      \
        }                                                                               \
    } while (false)

namespace pond::common {

template <typename T>
class Result {
public:
    // Constructors for success case
    Result(const T& value) : data_(value) {}
    Result(T&& value) : data_(std::move(value)) {}

    // Constructor for error case
    Result(const Error& error) : data_(error) {}
    Result(Error&& error) : data_(std::move(error)) {}

    // Check status
    [[nodiscard]] bool ok() const noexcept { return std::holds_alternative<T>(data_); }
    [[nodiscard]] bool hasError() const noexcept { return std::holds_alternative<Error>(data_); }

    // Access value
    [[nodiscard]] const T& value() const& {
        if (!ok()) {
            throw std::runtime_error("Attempting to access value of failed Result. Error: "
                                     + std::get<Error>(data_).message());
        }
        return std::get<T>(data_);
    }

    [[nodiscard]] T&& value() && {
        if (!ok()) {
            throw std::runtime_error("Attempting to access value of failed Result. Error: "
                                     + std::get<Error>(data_).message());
        }
        return std::move(std::get<T>(data_));
    }

    // Access error
    [[nodiscard]] const Error& error() const& {
        if (!hasError()) {
            throw std::runtime_error("Attempting to access error of successful Result");
        }
        return std::get<Error>(data_);
    }

    [[nodiscard]] Error&& error() && {
        if (!hasError()) {
            throw std::runtime_error("Attempting to access error of successful Result");
        }
        return std::move(std::get<Error>(data_));
    }

    // Static constructors
    [[nodiscard]] static Result<T> success(T value) { return Result<T>(std::move(value)); }

    [[nodiscard]] static Result<T> failure(ErrorCode code, std::string error_message) {
        return Result<T>(Error(code, std::move(error_message)));
    }

    [[nodiscard]] static Result<T> failure(ErrorCode code) { return Result<T>(Error(code)); }

    [[nodiscard]] static Result<T> failure(const Error& error) { return Result<T>(error); }

private:
    std::variant<T, Error> data_;
};

// Specialization for void
template <>
class Result<void> {
public:
    Result() : data_(std::monostate{}) {}
    Result(const Error& error) : data_(error) {}
    Result(Error&& error) : data_(std::move(error)) {}

    [[nodiscard]] bool ok() const noexcept { return std::holds_alternative<std::monostate>(data_); }
    [[nodiscard]] bool hasError() const noexcept { return std::holds_alternative<Error>(data_); }

    [[nodiscard]] const Error& error() const& {
        if (!hasError()) {
            throw std::runtime_error("Attempting to access error of successful Result");
        }
        return std::get<Error>(data_);
    }

    [[nodiscard]] Error&& error() && {
        if (!hasError()) {
            throw std::runtime_error("Attempting to access error of successful Result");
        }
        return std::move(std::get<Error>(data_));
    }

    [[nodiscard]] static Result<void> failure(ErrorCode code, std::string error_message) {
        return Result<void>(Error(code, std::move(error_message)));
    }

    [[nodiscard]] static Result<void> failure(ErrorCode code) { return Result<void>(Error(code)); }

    [[nodiscard]] static Result<void> failure(const Error& error) { return Result<void>(error); }

    [[nodiscard]] static Result<void> success() { return Result<void>(); }

private:
    std::variant<std::monostate, Error> data_;
};

// Detect if a type is already a common::Result.
template <typename T>
struct is_result_type : std::false_type {};

template <typename U>
struct is_result_type<Result<U>> : std::true_type {};

// Given a type T, if it's already a common::Result, leave it;
// otherwise, wrap it.
template <typename T, bool = is_result_type<T>::value>
struct as_result_type;

template <typename T>
struct as_result_type<T, true> {
    using type = T;
};

template <typename T>
struct as_result_type<T, false> {
    using type = Result<T>;
};

template <typename T>
using as_result_type_t = typename as_result_type<T>::type;

}  // namespace pond::common