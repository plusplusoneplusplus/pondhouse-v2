#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "common/result.h"
#include "rsm/replication.h"
#include "rsm/rsm.h"

namespace pond::rsm {

// Forward declaration
class MockReplicationOracle;

/**
 * Interceptor that hooks into a ReplicatedStateMachine's Replicate method to propagate
 * changes to secondaries via the MockReplicationOracle.
 */
class OracleReplicationInterceptor : public IReplicationInterceptor {
public:
    OracleReplicationInterceptor(std::shared_ptr<MockReplicationOracle> oracle) : oracle_(oracle) {}

    /**
     * Called by the primary ReplicatedStateMachine when it replicates data
     * @param data The data being replicated
     * @param primary_lsn The LSN assigned by the primary
     * @param callback Function to call when operation completes on all secondaries
     * @return Result containing success or error
     */
    common::Result<bool> OnPrimaryReplicate(const common::DataChunk& data,
                                            uint64_t primary_lsn,
                                            std::function<void()> callback = nullptr) override;

private:
    std::shared_ptr<MockReplicationOracle> oracle_;
};

/**
 * MockReplicationOracle manages multiple ReplicatedStateMachines, coordinating writes
 * between a primary and secondaries. It ensures that operations executed on the primary
 * are also applied to all secondaries in the same order.
 *
 * This is a simplified implementation for testing purposes.
 */
class MockReplicationOracle : public std::enable_shared_from_this<MockReplicationOracle> {
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
     * Intercepts a replication request from the primary and dispatches it to all secondaries
     * @param data The data to replicate to secondaries
     * @param primary_lsn The LSN assigned by the primary
     * @param callback Function to call when operation completes on all secondaries
     * @return Result containing success or error
     */
    common::Result<bool> PropagateToSecondaries(const common::DataChunk& data,
                                                uint64_t primary_lsn,
                                                std::function<void()> callback = nullptr);

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

    /**
     * Creates a new interceptor that can be attached to a ReplicatedStateMachine
     */
    std::shared_ptr<OracleReplicationInterceptor> CreateInterceptor();

private:
    std::shared_ptr<ReplicatedStateMachine> primary_;
    std::vector<std::shared_ptr<ReplicatedStateMachine>> secondaries_;
    mutable std::mutex mutex_;
    std::mutex replication_mutex_;  // Global lock to serialize all replications
    bool initialized_{false};
};

}  // namespace pond::rsm