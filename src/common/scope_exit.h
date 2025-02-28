#pragma once

#include <functional>
#include <utility>

namespace pond::common {

/**
 * ScopeExit is a RAII helper class that executes a function when it goes out of scope.
 * This is useful for cleanup operations that need to happen when exiting a scope,
 * whether through normal execution or due to an exception.
 *
 * Example usage:
 *   {
 *     auto cleanup = ScopeExit([&]() { cleanup_resource(); });
 *     // ... do work ...
 *   } // cleanup_resource() is called here when scope exits
 */
class ScopeExit {
public:
    template <typename F>
    explicit ScopeExit(F&& fn) : fn_(std::forward<F>(fn)) {}

    // Move constructor
    ScopeExit(ScopeExit&& other) noexcept : fn_(std::move(other.fn_)) { other.fn_ = nullptr; }

    // Destructor executes the cleanup function
    ~ScopeExit() {
        if (fn_)
            fn_();
    }

    // Disallow copying
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ScopeExit& operator=(ScopeExit&&) = delete;

private:
    std::function<void()> fn_;
};

}  // namespace pond::common
