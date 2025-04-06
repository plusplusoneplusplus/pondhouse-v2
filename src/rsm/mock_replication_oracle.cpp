#include "rsm/mock_replication_oracle.h"

#include "common/error.h"
#include "common/log.h"

namespace pond::rsm {

using namespace common;

Result<bool> MockReplicationOracle::Initialize() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Oracle already initialized");
    }

    initialized_ = true;
    return Result<bool>::success(true);
}

Result<bool> MockReplicationOracle::RegisterPrimary(std::shared_ptr<ReplicatedStateMachine> state_machine) {
    if (!state_machine) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "State machine cannot be null");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Oracle not initialized");
    }

    if (primary_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Primary already registered");
    }

    // Check if this state machine is already registered as a secondary
    for (const auto& secondary : secondaries_) {
        if (secondary == state_machine) {
            return Result<bool>::failure(ErrorCode::InvalidOperation, "Cannot register a secondary as primary");
        }
    }

    primary_ = state_machine;
    LOG_INFO("Registered primary state machine");

    return Result<bool>::success(true);
}

Result<bool> MockReplicationOracle::RegisterSecondary(std::shared_ptr<ReplicatedStateMachine> state_machine) {
    if (!state_machine) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "State machine cannot be null");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Oracle not initialized");
    }

    // Check if this state machine is already the primary
    if (primary_ == state_machine) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Cannot register primary as secondary");
    }

    // Check if this state machine is already registered as a secondary
    for (const auto& secondary : secondaries_) {
        if (secondary == state_machine) {
            return Result<bool>::success(false);  // Already registered
        }
    }

    secondaries_.push_back(state_machine);
    LOG_INFO("Registered secondary state machine, total secondaries: %zu", secondaries_.size());

    return Result<bool>::success(true);
}

Result<bool> MockReplicationOracle::UnregisterStateMachine(std::shared_ptr<ReplicatedStateMachine> state_machine) {
    if (!state_machine) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "State machine cannot be null");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Oracle not initialized");
    }

    // Check if it's the primary
    if (primary_ == state_machine) {
        primary_ = nullptr;
        LOG_INFO("Unregistered primary state machine");
        return Result<bool>::success(true);
    }

    // Check if it's a secondary
    for (auto it = secondaries_.begin(); it != secondaries_.end(); ++it) {
        if (*it == state_machine) {
            secondaries_.erase(it);
            LOG_INFO("Unregistered secondary state machine, remaining secondaries: %zu", secondaries_.size());
            return Result<bool>::success(true);
        }
    }

    return Result<bool>::success(false);  // Not found
}

Result<uint64_t> MockReplicationOracle::Replicate(const DataChunk& data, std::function<void()> callback) {
    std::shared_ptr<ReplicatedStateMachine> primary;
    std::vector<std::shared_ptr<ReplicatedStateMachine>> secondaries_copy;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!initialized_) {
            return Result<uint64_t>::failure(ErrorCode::InvalidOperation, "Oracle not initialized");
        }

        if (!primary_) {
            return Result<uint64_t>::failure(ErrorCode::InvalidOperation, "No primary registered");
        }

        primary = primary_;
        secondaries_copy = secondaries_;
    }

    // Acquire the global replication lock to serialize all operations
    std::lock_guard<std::mutex> replication_lock(replication_mutex_);

    // Keep track of total number of pending replication operations
    size_t total_replicas = 1 + secondaries_copy.size();  // Primary + all secondaries
    size_t completed_replicas = 0;
    std::mutex completion_mutex;
    std::condition_variable completion_cv;

    // Create a tracking function for completion
    auto track_completion = [&]() {
        std::lock_guard<std::mutex> lock(completion_mutex);
        completed_replicas++;
        if (completed_replicas == total_replicas) {
            completion_cv.notify_one();
        }
    };

    // First, replicate to the primary
    uint64_t primary_lsn = INVALID_LSN;
    auto primary_result = primary->Replicate(data, track_completion);

    if (!primary_result.ok()) {
        LOG_ERROR("Failed to replicate to primary: %s", primary_result.error().message().c_str());
        return Result<uint64_t>::failure(primary_result.error());
    }

    // Now replicate to all secondaries
    bool any_failure = false;

    for (auto& secondary : secondaries_copy) {
        auto secondary_result = secondary->Replicate(data, track_completion);

        if (!secondary_result.ok()) {
            LOG_ERROR("Failed to replicate to a secondary: %s", secondary_result.error().message().c_str());
            any_failure = true;

            // Count failed replications as completed for tracking purposes
            std::lock_guard<std::mutex> lock(completion_mutex);
            completed_replicas++;

            // Continue with other secondaries
        }
    }

    // Wait for all replications to complete
    {
        std::unique_lock<std::mutex> lock(completion_mutex);
        completion_cv.wait(lock, [&] { return completed_replicas == total_replicas; });
    }

    // Now that all replications have completed, get the primary's LSN
    primary_lsn = primary->GetLastExecutedLSN();

    // Call the callback after all replications are complete
    if (callback) {
        callback();
    }

    // Return the primary's LSN
    return Result<uint64_t>::success(primary_lsn);
}

size_t MockReplicationOracle::GetSecondaryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return secondaries_.size();
}

std::shared_ptr<ReplicatedStateMachine> MockReplicationOracle::GetPrimary() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return primary_;
}

}  // namespace pond::rsm