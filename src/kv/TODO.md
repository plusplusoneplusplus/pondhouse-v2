## TODO List

### 1. SSTable Reader Implementation [COMPLETED]
- [x] Create `SSTableReader` class
  - [x] File validation and header parsing
  - [x] Index block loading and caching
  - [x] Filter block loading and caching
  - [x] Data block reading with decompression support
- [x] Add iterator support
  - [x] Block-level iteration
  - [x] Key seeking functionality
  - [x] Sequential scan optimization
- [x] Write comprehensive tests
  - [x] Random access patterns
  - [x] Sequential scan patterns
  - [x] Cache hit/miss scenarios
  - [x] Concurrent access tests

### 2. Version Management [MEDIUM PRIORITY]
- [x] Implement version tracking
  - [x] SSTable versioning with InternalKey
  - [x] Version-aware reads and writes
  - [x] Iterator support for versions
  - [x] Version ordering in SSTable
  - [ ] Level management
  - [ ] Manifest file format
- [ ] Add recovery mechanisms
  - [ ] Manifest parsing
  - [ ] Consistency checking
  - [ ] Error recovery
- [ ] Handle concurrent operations
  - [x] Version handling during reads
  - [x] Version ordering validation
  - [ ] Garbage collection
  - [ ] File cleanup
- [x] Add version-specific features
  - [x] Time-travel queries
  - [x] Version iteration
  - [x] Version comparison
  - [x] Multi-version SSTable support
  - [x] Version visibility filtering

### 3. SSTableManager Implementation [IN PROGRESS]

The `SSTableManager` class manages the lifecycle and organization of SSTables with the following features:

#### Core Components
- Level-based SSTable organization
- Thread-safe operations with shared mutex protection
- Block cache management
- File system abstraction via `IAppendOnlyFileSystem`
- Multi-version support with timestamp-based visibility

#### Key Features

1. **Thread Safety**
   - Shared mutex for concurrent read operations
   - Exclusive locks for write operations
   - Safe concurrent access to SSTable metadata
   - Protected statistics updates

2. **Level Management**
   - L0 tables with potential key range overlap
   - Leveled organization for L1+ with non-overlapping ranges
   - Atomic file number generation
   - Safe level creation and expansion

3. **Read Path**
   - Concurrent read support via shared locks
   - L0 search from newest to oldest
   - Level-based search with range filtering
   - Block cache integration
   - Version-aware reads with timestamp filtering

4. **Write Path**
   - Atomic MemTable to SSTable conversion
   - Protected file number allocation
   - Safe L0 table addition
   - Automatic statistics update
   - Version preservation during flush

5. **Version Management**
   - Descending version order within SSTable
   - Version visibility based on read timestamp
   - Efficient version filtering during reads
   - Version-aware iterator implementation
   - Skip optimization for invisible versions

#### Implementation Status
- [x] Basic SSTable organization
- [x] Thread-safe read operations
- [x] Thread-safe write operations
- [x] Statistics tracking
- [x] Directory recovery
- [x] Multi-version support
- [x] L0 to L1 merge implementation
- [x] Key range overlap handling
- [x] Version preservation during merge
- [ ] Background compaction
  - [ ] Compaction worker implementation
  - [ ] Job queue management
  - [ ] Progress tracking
  - [ ] Error handling and recovery
- [ ] Level size limits
  - [ ] Size-based compaction triggers
  - [ ] Level growth management
  - [ ] Dynamic level count
- [ ] Automatic compaction triggering
  - [ ] L0 file count triggers
  - [ ] Level size ratio triggers
  - [ ] Write amplification control
- [ ] Version garbage collection
  - [ ] Obsolete version identification
  - [ ] Safe version removal
  - [ ] GC during compaction

#### Compaction Tasks [NEW]
1. Background Processing
   - [ ] Implement CompactionWorker class
   - [ ] Add job priority queue
   - [ ] Add progress monitoring
   - [ ] Implement graceful shutdown

2. Multi-Level Support
   - [ ] Implement L1 to L2 compaction
   - [ ] Add level size ratio management
   - [ ] Support dynamic level creation
   - [ ] Add level-specific compaction strategies

3. Resource Management
   - [ ] Add I/O rate limiting
   - [ ] Implement memory budget
   - [ ] Add CPU usage controls
   - [ ] Monitor disk space usage

4. Metrics and Monitoring
   - [ ] Track compaction statistics
   - [ ] Monitor write amplification
   - [ ] Track compaction latency
   - [ ] Add progress reporting

#### Usage Example
```cpp
// Create manager with configuration
SSTableManager manager(fs, "db_path", config);

// Thread-safe read operations with version
auto result = manager.Get(key, timestamp);

// Thread-safe write operations with version
auto memtable = CreateMemTable();
manager.CreateSSTableFromMemTable(memtable);  // Preserves versions

// Get current statistics
auto stats = manager.GetStats();
```

### 4. Write Path Integration [HIGH PRIORITY]
- [x] Write-Ahead Log (WAL) Implementation
  - [x] Recovery replay logic
    - [x] Partial record handling
    - [x] State reconstruction

- [x] MemTable Management
  - [x] Implement MemTable switching
    - [x] Active and immutable table management
    - [x] Thread-safe table transitions
    - [x] Reference counting for safe deletion
  - [x] Add flush triggers
    - [x] Size-based threshold
    - [ ] Time-based periodic flushes
    - [x] Manual flush support
  - [x] Concurrent access handling
    - [x] Read/Write synchronization
    - [x] Switch coordination

- [x] Background Flush Mechanism
  - [x] Background worker implementation
    - [x] Flush thread management
    - [x] Work queue handling
    - [x] Graceful shutdown
  - [x] Flush coordination
    - [x] Progress tracking
    - [x] Error handling and retry logic
    - [x] Temporary file management
  - [ ] Monitoring and metrics
    - [ ] Flush latency tracking
    - [ ] Queue size monitoring
    - [ ] Error rate tracking

- [x] Sequence Number Management
  - [x] Atomic sequence generation
  - [x] Integration points
    - [x] WAL records
    - [x] MemTable entries
    - [x] SSTable metadata
  - [x] Consistency guarantees
    - [x] Cross-table ordering
    - [x] Recovery ordering

### 5. Read Path Integration [HIGH PRIORITY]
- [x] Implement merged iteration
  - [x] MemTable/SSTable merger
  - [x] Version snapshot reading
  - [x] Iterator interface
- [ ] Add block cache management
  - [ ] Cache policy implementation
  - [ ] Memory budget
  - [ ] Eviction strategy
- [ ] Optimize read patterns
  - [x] Bloom filter utilization
  - [ ] Read-ahead for scans
  - [ ] Cache warmup strategy

### 6. Schema-Agnostic Storage Layer [COMPLETED]
- [x] Implement KvTable base class
  - [x] Raw byte storage interface
  - [x] Thread-safe operations
  - [x] WAL management
  - [x] SSTable integration
- [x] Refactor Table class
  - [x] Inherit from KvTable
  - [x] Add schema validation layer
  - [x] Implement Record serialization
  - [x] Add column operations

### 7. Performance Optimization [LOW PRIORITY]
- [ ] Benchmark suite
  - [ ] Read/write benchmarks
  - [ ] Compaction benchmarks
  - [ ] Cache effectiveness tests
- [ ] Profiling and optimization
  - [ ] CPU profiling
  - [ ] Memory profiling
  - [ ] I/O profiling
- [ ] Tuning parameters
  - [ ] Block sizes
  - [ ] Cache sizes
  - [ ] Compression settings

### 8. Future Enhancements [LOW PRIORITY]
- [ ] Add transaction support
  - [ ] MVCC implementation
  - [ ] Deadlock detection
  - [ ] Isolation levels
- [ ] Implement advanced features
  - [ ] Range queries
  - [ ] Secondary indices
  - [ ] Column families
- [ ] Add monitoring and management
  - [ ] Table statistics
  - [ ] Resource usage tracking
  - [ ] Health checks

### 9. MVCC Implementation [HIGH PRIORITY]

#### Phase 1: Version Chain Structure [COMPLETED]
- [x] Implement `VersionedValue` template class
  - [x] Generic value type support
  - [x] Version number and timestamp
  - [x] Transaction ID
  - [x] Deletion marker
  - [x] Previous version pointer with const correctness
  - [x] Value visibility by timestamp
  - [x] Value visibility by transaction ID
  - [x] Version chain creation and management
  - [x] Comprehensive test coverage
- [x] Add version chain support to MemTable
  - [x] Store `VersionedValue` in SkipList
  - [x] Add version visibility logic
  - [x] Update memory usage calculation
  - [x] Thread-safe version chain operations
  - [x] Support for deletion markers
  - [x] Iterator support for versioned values
  - [x] Comprehensive test coverage

#### Phase 2: Transaction Management [IN PROGRESS]
- [ ] Implement `Transaction` class
  - [ ] Transaction ID generation
  - [ ] Start timestamp
  - [ ] Commit timestamp
  - [ ] Write set tracking
  - [ ] Read set tracking
  - [ ] Status (active/committed/aborted)
- [ ] Add transaction manager to KvTable
  - [ ] Active transaction tracking
  - [ ] Timestamp allocation
  - [ ] Deadlock detection
  - [ ] Cleanup of old versions

#### Phase 3: MVCC Operations
- [ ] Modify KvTable operations
  - [ ] Add transaction context to Put/Get/Delete
  - [ ] Implement snapshot reads
  - [ ] Add version visibility checks
  - [ ] Handle write conflicts
- [ ] Add new transaction operations
  - [ ] Begin transaction
  - [ ] Commit transaction
  - [ ] Rollback transaction
  - [ ] Create snapshot

#### Phase 4: SSTable Integration
- [ ] Extend SSTable format
  - [ ] Add version information to entries
  - [ ] Modify block format for version chains
  - [ ] Update bloom filters for versions
- [ ] Update SSTable writer
  - [ ] Write version information
  - [ ] Handle version chains
  - [ ] Optimize version storage
- [ ] Update SSTable reader
  - [ ] Read version information
  - [ ] Filter versions by timestamp
  - [ ] Handle version visibility

#### Phase 5: Garbage Collection
- [ ] Implement version GC
  - [ ] Track oldest active transaction
  - [ ] Identify obsolete versions
  - [ ] Safe version removal
  - [ ] Background cleanup process
- [ ] Add GC policies
  - [ ] Time-based cleanup
  - [ ] Space-based cleanup
  - [ ] Version chain length limits

#### Phase 6: Recovery & Durability
- [ ] Update WAL format
  - [ ] Add transaction records
  - [ ] Store version information
  - [ ] Track transaction status
- [ ] Enhance recovery process
  - [ ] Transaction status recovery
  - [ ] Version chain reconstruction
  - [ ] Handle in-doubt transactions

#### Phase 7: Testing & Validation
- [x] Unit tests for VersionedValue
  - [x] Version chain operations
  - [x] Timestamp-based visibility
  - [x] Transaction-based visibility
  - [x] Deletion markers
  - [x] Template type support
- [ ] Integration tests
  - [ ] Multi-version SSTable
  - [ ] GC effectiveness
  - [ ] Recovery correctness
- [ ] Performance tests
  - [ ] Version chain overhead
  - [ ] Transaction throughput
  - [ ] GC impact

### Expected Usage Example:
```cpp
// Start a transaction
auto txn = table.BeginTransaction();

// Read with snapshot isolation
auto snapshot = table.CreateSnapshot();
auto value1 = table.Get("key1", snapshot);

// Write within transaction
table.Put("key2", value2, txn);

// Commit or rollback
if (success) {
    txn.Commit();
} else {
    txn.Rollback();
}

// Read latest committed version
auto latest = table.Get("key2");

// Read specific version
auto old_version = table.Get("key2", timestamp);
```

### Implementation Order:
1. ✓ Version chain structure (foundation)
2. Basic transaction management
3. MVCC read/write operations
4. SSTable format updates
5. Garbage collection
6. Recovery enhancements
7. Testing and optimization

This will be implemented incrementally, with each phase building on the previous ones while maintaining backward compatibility with existing functionality.
