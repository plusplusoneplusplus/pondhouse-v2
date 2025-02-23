# Replicated State Machine (RSM)

## Overview
The Replicated State Machine (RSM) component provides a framework for implementing distributed state machines with consistent replication across nodes. It combines both replication and execution to ensure all nodes maintain the same state.

## Current Implementation

### Core Components

#### 1. Interfaces
- `IReplication`: Interface for replicating log entries across nodes
- `IReplicatedStateMachine`: Interface combining replication and execution
- `IReplicatedLogExecutor`: Interface for executing committed log entries

#### 2. WAL-based Implementation
- `WalReplication`: Implementation of replication using Write-Ahead Log (WAL)
  - Provides durability through persistent logging
  - Supports log entry appending and reading
  - Maintains LSN (Log Sequence Number) tracking

#### 3. Base Implementation
- `ReplicatedStateMachine`: Base implementation of IReplicatedStateMachine
  - Background execution thread for committed entries
  - Queue-based execution ordering
  - Support for draining pending entries

#### 4. Snapshot Support
- `ISnapshotable`: Interface for snapshot operations
- `ISnapshotManager`: Interface for snapshot management
- `FileSystemSnapshotManager`: Implementation using filesystem
  - Footer-based snapshot format
  - Atomic snapshot creation
  - Metadata tracking
  - Retention policy support

#### 5. I/O Abstractions
- Stream interfaces for data handling
  - `InputStream`/`OutputStream` base interfaces
  - Memory-based implementation
  - Filesystem-based implementation

## TODO List

### High Priority
1. **Consensus Protocol**
   - [ ] Implement Raft consensus algorithm
   - [ ] Add leader election
   - [ ] Implement log replication across nodes
   - [ ] Add membership changes

2. **State Transfer**
   - [x] Implement snapshot mechanism
   - [ ] Add incremental state transfer
   - [ ] Support catch-up for lagging nodes

3. **Fault Tolerance**
   - [ ] Add failure detection
   - [ ] Implement node recovery
   - [ ] Handle network partitions

### Medium Priority
1. **Performance Improvements**
   - [ ] Batch processing for log entries
   - [ ] Optimize WAL for sequential writes
   - [ ] Add caching layer for frequently accessed state

2. **Monitoring & Observability**
   - [x] Add execution notification hooks
   - [ ] Add metrics collection
   - [ ] Implement health checks
   - [ ] Add tracing support

3. **Configuration Management**
   - [ ] Dynamic configuration updates
   - [ ] Configuration validation
   - [ ] Safe reconfiguration protocol

### Low Priority
1. **Tooling**
   - [ ] Add admin CLI
   - [ ] Create visualization tools
   - [ ] Develop testing tools

2. **Documentation**
   - [ ] Add API documentation
   - [ ] Create deployment guide
   - [ ] Write troubleshooting guide

3. **Additional Features**
   - [ ] Read-only replicas
   - [ ] Custom serialization formats
   - [ ] Pluggable storage backends

## Usage Example

```cpp
// Create and initialize RSM
auto rsm = std::make_shared<ReplicatedStateMachine>(replication);
rsm->Initialize(config);

// Replicate data
DataChunk data = ...;
auto result = rsm->Replicate(data);

// Wait for execution
auto last_executed = rsm->GetLastExecutedLSN();
```

## Contributing
When contributing to the RSM component:
1. Add unit tests for new functionality
2. Update documentation for interface changes
3. Follow the existing code style
4. Consider backward compatibility
5. Add appropriate synchronization for thread safety

## Testing
The component includes comprehensive tests:
- Unit tests for individual components
- Integration tests for end-to-end flows
- Parameterized tests for different implementations
- Synchronization-based tests for concurrent operations
- Snapshot and recovery tests 