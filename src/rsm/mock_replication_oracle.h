#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/result.h"
#include "rsm/replication.h"
#include "rsm/rsm.h"

namespace pond::rsm {

/**
 * MockReplicationOracle manages multiple ReplicatedStateMachines, coordinating writes
 * between a primary and secondaries. It ensures that operations executed on the primary
 * are also applied to all secondaries in the same order.
 *
 * This is a simplified implementation for testing purposes.
 */
class MockReplicationOracle {
public:
    MockReplicationOracle() = default;
    ~MockReplicationOracle() = default;

    /**
     * Initializes the oracle
     */
    common::Result<bool> Initialize();

    /**
     * Registers a ReplicatedStateMachine as the primary
     * @param state_machine The state machine to register as primary
     * @return Result containing success or error
     */
    common::Result<bool> RegisterPrimary(std::shared_ptr<ReplicatedStateMachine> state_machine);

    /**
     * Registers a ReplicatedStateMachine as a secondary
     * @param state_machine The state machine to register as secondary
     * @return Result containing success or error
     */
    common::Result<bool> RegisterSecondary(std::shared_ptr<ReplicatedStateMachine> state_machine);

    /**
     * Unregisters a ReplicatedStateMachine
     * @param state_machine The state machine to unregister
     * @return Result containing success or error
     */
    common::Result<bool> UnregisterStateMachine(std::shared_ptr<ReplicatedStateMachine> state_machine);

    /**
     * Replicates data to the primary and propagates to all secondaries
     * @param data The data to replicate
     * @param callback Function to call when operation completes on all nodes
     * @return Result containing the operation LSN or error
     */
    common::Result<uint64_t> Replicate(const common::DataChunk& data, std::function<void()> callback = nullptr);

    /**
     * Gets the number of registered secondaries
     * @return Number of secondaries
     */
    size_t GetSecondaryCount() const;

    /**
     * Gets the primary state machine
     * @return Shared pointer to the primary state machine or nullptr if none
     */
    std::shared_ptr<ReplicatedStateMachine> GetPrimary() const;

private:
    std::shared_ptr<ReplicatedStateMachine> primary_;
    std::vector<std::shared_ptr<ReplicatedStateMachine>> secondaries_;
    mutable std::mutex mutex_;
    std::mutex replication_mutex_;  // Global lock to serialize all replications
    bool initialized_{false};
};

}  // namespace pond::rsm