#pragma once

#include <concepts>
#include <format>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <functional>

#include "error.h"

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
            throw std::runtime_error(std::format("Attempting to access value of failed Result. Error: {}", 
                                               std::get<Error>(data_).message()));
        }
        return std::get<T>(data_);
    }

    [[nodiscard]] T&& value() && {
        if (!ok()) {
            throw std::runtime_error(std::format("Attempting to access value of failed Result. Error: {}", 
                                               std::get<Error>(data_).message()));
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
    [[nodiscard]] static Result<T> success(T value) {
        return Result<T>(std::move(value));
    }

    [[nodiscard]] static Result<T> failure(ErrorCode code, std::string error_message) {
        return Result<T>(Error(code, std::move(error_message)));
    }

    [[nodiscard]] static Result<T> failure(ErrorCode code) {
        return Result<T>(Error(code));
    }

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

private:
    std::variant<std::monostate, Error> data_;
};

} // namespace pond::common