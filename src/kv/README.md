# Key-Value Store Implementation

This directory contains the implementation of a Log-Structured Merge-tree (LSM) based key-value store, with MemTable for in-memory storage and SSTable (Sorted String Table) for persistent storage.

## MemTable

The MemTable is an in-memory data structure that provides fast read and write operations. It serves as the first level of storage in our LSM-tree implementation.

### Design

#### Core Components
- Schema-aware storage using `Record` class
- Thread-safe operations with mutex protection
- Memory usage tracking
- Configurable size limits

#### Key Features

1. **Thread Safety**
   - All operations are protected by mutex
   - Concurrent read/write support
   - Safe iterator implementation

2. **Memory Management**
   - Configurable maximum size (default provided)
   - Memory usage tracking per entry
   - Automatic flush triggering when size limit is reached

3. **Operations**
   - Put: Insert or update records
   - Get: Retrieve records by key
   - Delete: Remove records (using tombstones)
   - Column operations: Update/retrieve individual columns

4. **Schema Support**
   - Type-safe column operations
   - Schema validation on record insertion
   - Null value support

### Usage

```cpp
// Create a MemTable with schema and size limit
MemTable table(schema, max_size);

// Basic operations
table.Put(key, record);
table.Get(key);
table.Delete(key);

// Column operations
table.UpdateColumn(key, column_name, value);
table.GetColumn(key, column_name);

// Memory management
if (table.ShouldFlush()) {
    // Flush to SSTable
}
```

### Implementation Details

1. **Memory Tracking**
   - Tracks approximate memory usage
   - Includes key size, record size, and overhead
   - Atomic updates for thread safety

2. **Deletion Handling**
   - Uses tombstone records
   - Null values for all columns indicate deletion
   - Preserves deletion order for compaction

3. **Iterator Support**
   - Thread-safe iteration
   - Seek operation for efficient range queries
   - Valid/Next operations for traversal

4. **Recovery**
   - WAL (Write-Ahead Log) integration
   - Crash recovery support
   - Consistent state restoration

## SSTable (Sorted String Table)

## SSTable Format

The SSTable implementation follows a block-based design with the following components:

### File Layout
```
+----------------------------------------------------------------------------------------+
| File Header (64 bytes)                                                                 |
+----------------------------------------------------------------------------------------+
| Magic (8B) | Version (4B) | Flags (4B) | Reserved (48B)                                |

+----------------------------------------------------------------------------------------+
| Data Blocks (variable size)                                                            |
+----------------------------------------------------------------------------------------+
| Data Block 1    | Data Block 2    | ... | Data Block N    |                            |
| [Key-Value      | [Key-Value      | ... | [Key-Value      |                            |
|  Entries...]    |  Entries...]    | ... |  Entries...]    |                            |
| [Block Footer]  | [Block Footer]  | ... | [Block Footer]  |                            |

+----------------------------------------------------------------------------------------+
| Index Block                                                                            |
+----------------------------------------------------------------------------------------+
| Index Entry 1   | Index Entry 2    | ... | Index Entry N    | Index Footer             |
| - Key Length    | - Key Length     | ... | - Key Length     | - Entry Count            |
| - Largest Key   | - Largest Key    | ... | - Largest Key    | - Checksum               |
| - Block Offset  | - Block Offset   | ... | - Block Offset   |                          |
| - Block Size    | - Block Size     | ... | - Block Size     |                          |

+----------------------------------------------------------------------------------------+
| Filter Block (optional)                                                                |
+----------------------------------------------------------------------------------------+
| Bloom Filter Data | Filter Footer  |                                                   |
| [Filter Bits]     | - Size         |                                                   |
|                   | - Checksum     |                                                   |

+----------------------------------------------------------------------------------------+
| Metadata Block                                                                         |
+----------------------------------------------------------------------------------------+
| Stats              | Properties        |                                               |
| - Key Count        | - Creation Time   |                                               |
| - Min/Max Key      | - Compression     |                                               |
| - Size Stats       | - Index Type      |                                               |

+----------------------------------------------------------------------------------------+
| Footer (64 bytes)                                                                      |
+----------------------------------------------------------------------------------------+
| Index Block    | Filter Block   | Metadata Block  | Padding     | Magic      |         |
| Offset (8B)    | Offset (8B)    | Offset (8B)     | (32B)       | Number (8B)|         |
```

### Key Components

#### 1. File Header
- 64-byte header containing file metadata
- Magic number for file identification
- Version information
- Feature flags (e.g., compression, bloom filter)
- Reserved space for future extensions

#### 2. Data Blocks
- Variable-sized blocks containing sorted key-value pairs
- Each block has a footer with:
  - Entry count
  - Block size
  - CRC32 checksum
  - Compression type

#### 3. Index Block
- Contains entries pointing to data blocks
- Each entry includes:
  - Largest key in the referenced block
  - Block offset and size
  - Entry count

#### 4. Filter Block
- Optional Bloom filter for efficient key lookups
- Helps avoid unnecessary disk reads
- Contains:
  - Bloom filter bits
  - Footer with size and checksum
  - Configurable false positive rate

#### 5. Metadata Block
- Contains table statistics and properties
- Includes:
  - Total key count
  - Min/Max keys
  - Size statistics
  - Creation time
  - Compression settings

#### 6. Footer
- 64-byte footer with offsets to:
  - Index block
  - Filter block
  - Metadata block
- Magic number for integrity verification

### Features

1. **Block-based Structure**
   - Configurable block size (default 4MB)
   - Independent compression of blocks
   - Efficient random access

2. **Bloom Filters**
   - Configurable false positive rate
   - Reduces unnecessary disk I/O
   - Optional per-block filters

3. **Data Integrity**
   - CRC32 checksums for data blocks
   - Magic numbers for file integrity
   - Version checking

4. **Performance Optimizations**
   - Block caching
   - Prefix compression
   - Efficient binary search in index blocks

## Usage

The SSTable implementation provides builders for constructing tables:

```cpp
// Create a data block builder
DataBlockBuilder data_builder(kTargetBlockSize);
data_builder.Add(key, value);

// Create a filter block builder
FilterBlockBuilder filter_builder(expected_keys, false_positive_rate);
filter_builder.AddKeys(keys);

// Create an index block builder
IndexBlockBuilder index_builder;
index_builder.AddEntry(largest_key, block_offset, block_size, entry_count);
```

## Implementation Notes

1. All multi-byte integers are stored in little-endian format
2. String lengths are stored as fixed-size 32-bit integers
3. Block sizes are optimized for common storage block sizes
4. The implementation supports future extensions through reserved fields

## Table

The `Table` class is the main entry point for the key-value store, managing the lifecycle of MemTables and SSTables while providing a simple interface for data operations.

### Architecture

#### Components
- Schema management for type-safe operations
- Active MemTable for in-memory operations
- Write-Ahead Log (WAL) for durability
- SSTable management (planned)
- Filesystem abstraction via `IAppendOnlyFileSystem`

#### Key Features

1. **Data Operations**
   - Put: Insert or update records
   - Get: Retrieve records by key
   - Delete: Remove records
   - Column-level updates

2. **Durability**
   - Write-Ahead Logging
   - Crash recovery support
   - Atomic operations

3. **Resource Management**
   - MemTable lifecycle
   - SSTable organization
   - File system interactions

### Usage

```cpp
// Create a table with schema and filesystem
Table table(schema, fs, "my_table");

// Basic operations
table.Put(key, record);
table.Get(key);
table.Delete(key);

// Column operations
table.UpdateColumn(key, "column_name", value);

// Maintenance
table.Flush();  // Flush MemTable to SSTable
table.Recover();  // Recover from crash
```

### Implementation Details

1. **Write Path**
   - Writes go to WAL first
   - Then to active MemTable
   - Background flush to SSTable when needed

2. **Read Path**
   - Check active MemTable first
   - Then search in SSTables
   - Merge results if needed

3. **Recovery**
   - Read WAL entries
   - Rebuild MemTable state
   - Verify SSTable consistency

## Testing

The implementation includes comprehensive tests covering:
- Basic operations
- Data integrity
- Filter block functionality
- Block builder operations
- Error handling
- Edge cases

## TODO List

### 1. SSTable Reader Implementation [COMPLETED]
- [x] Create `SSTableReader` class
  - [x] File validation and header parsing
  - [x] Index block loading and caching
  - [x] Filter block loading and caching
  - [x] Data block reading with decompression support
- [ ] Implement block cache
  - [ ] LRU cache policy
  - [ ] Cache size management
  - [ ] Thread-safe operations
- [x] Add iterator support
  - [x] Block-level iteration
  - [x] Key seeking functionality
  - [x] Sequential scan optimization
- [x] Write comprehensive tests
  - [x] Random access patterns
  - [x] Sequential scan patterns
  - [x] Cache hit/miss scenarios
  - [x] Concurrent access tests

### SSTableReader Implementation Details

The `SSTableReader` class provides efficient read access to SSTable files with the following features:

#### Key Components
- File header and footer validation
- Index block for efficient key lookup
- Optional bloom filter support for fast key existence checks
- Data block parsing and validation

#### Key Features
1. **Random Access**
   - O(log n) key lookup using index block
   - Binary search within data blocks
   - Bloom filter optimization for non-existent keys

2. **Data Integrity**
   - CRC32 checksum validation
   - Block size verification
   - File format validation

3. **Memory Efficiency**
   - On-demand block loading
   - No unnecessary data caching
   - Minimal memory footprint

4. **Performance Optimizations**
   - Bloom filter for fast negative lookups
   - Binary search in index and data blocks
   - Efficient key range filtering

#### Usage Example
```cpp
// Create reader
SSTableReader reader(fs, "data.sst");
ASSERT_TRUE(reader.Open().ok());

// Get value by key
auto result = reader.Get("key1");
if (result.ok()) {
    // Process value
    auto value = result.value();
}

// Check key existence (with bloom filter)
auto may_contain = reader.MayContain("key2");
if (may_contain.ok() && may_contain.value()) {
    // Key might exist
}

// Get metadata
size_t num_entries = reader.GetEntryCount();
size_t file_size = reader.GetFileSize();
std::string smallest = reader.GetSmallestKey();
std::string largest = reader.GetLargestKey();
```

### 2. Compaction Manager [MEDIUM PRIORITY]
- [ ] Design compaction strategies
  - [ ] Leveled compaction
  - [ ] Size-tiered compaction
  - [ ] Custom policies support
- [ ] Implement background compaction
  - [ ] Compaction worker threads
  - [ ] I/O throttling
  - [ ] Progress tracking
- [ ] Handle overlapping ranges
  - [ ] Key range calculation
  - [ ] Merge strategy
  - [ ] Tombstone cleanup
- [ ] Add monitoring and metrics
  - [ ] Compaction statistics
  - [ ] Performance metrics
  - [ ] Space amplification tracking

### 3. Version Management [MEDIUM PRIORITY]
- [ ] Implement version tracking
  - [ ] SSTable versioning
  - [ ] Level management
  - [ ] Manifest file format
- [ ] Add recovery mechanisms
  - [ ] Manifest parsing
  - [ ] Consistency checking
  - [ ] Error recovery
- [ ] Handle concurrent operations
  - [ ] Version changes during reads
  - [ ] Garbage collection
  - [ ] File cleanup

### 4. Write Path Integration [HIGH PRIORITY]
- [ ] Implement MemTable flushing
  - [ ] Flush triggers
  - [ ] SSTable creation
  - [ ] Concurrent flush handling
- [ ] Add Write-Ahead Log
  - [ ] WAL format
  - [ ] Log rotation
  - [ ] Recovery replay
- [ ] Sequence number management
  - [ ] Atomic sequence generation
  - [ ] Consistency guarantees
  - [ ] Cleanup policy

### 5. Read Path Integration [HIGH PRIORITY]
- [ ] Implement merged iteration
  - [ ] MemTable/SSTable merger
  - [ ] Version snapshot reading
  - [ ] Iterator interface
- [ ] Add block cache management
  - [ ] Cache policy implementation
  - [ ] Memory budget
  - [ ] Eviction strategy
- [ ] Optimize read patterns
  - [ ] Bloom filter utilization
  - [ ] Read-ahead for scans
  - [ ] Cache warmup strategy

### 6. Table Implementation [HIGH PRIORITY]
- [ ] Complete core Table operations
  - [ ] Implement SSTable list management
  - [ ] Add background flush mechanism
  - [ ] Handle MemTable switching during flush
  - [ ] Implement proper error handling
- [ ] Add transaction support
  - [ ] Begin/Commit/Rollback operations
  - [ ] MVCC implementation
  - [ ] Deadlock detection
- [ ] Implement advanced features
  - [ ] Range queries
  - [ ] Batch operations
  - [ ] Atomic updates
  - [ ] Secondary indices
- [ ] Add monitoring and management
  - [ ] Table statistics
  - [ ] Resource usage tracking
  - [ ] Background task management
  - [ ] Health checks

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
