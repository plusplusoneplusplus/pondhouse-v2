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
| Footer (128 bytes)                                                                      |
+----------------------------------------------------------------------------------------+
| Index Block    | Filter Block   | Metadata Block  | Padding     | Magic      |         |
| Offset (8B)    | Offset (8B)    | Offset (8B)     | (72B)       | Number (8B)|         |
| Size (8B)      | Size (8B)      | Size (8B)       |             |            |         |
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
- Stats section includes:
  - Total key count
  - Min/Max keys
  - Total key size
  - Total value size
- Properties section includes:
  - Creation timestamp
  - Compression settings
  - Index type
  - Filter configuration
- CRC32 checksum validation

#### 6. Footer
- 128-byte footer containing:
  - Index block offset and size
  - Filter block offset and size
  - Metadata block offset and size
  - 72-byte padding for future extensions
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

5. **Future Enhancements** [PLANNED]
   - Block compression
   - Advanced caching strategies
   - Performance optimizations
   - Additional filter types

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

### SSTableReader Implementation Details

The `SSTableReader` class provides efficient read access to SSTable files with the following features:

#### Key Components
- File header and footer validation
- Index block for efficient key lookup
- Optional bloom filter support for fast key existence checks

## Multi-Version Concurrency Control (MVCC)

The key-value store implements Multi-Version Concurrency Control to provide snapshot isolation and concurrent access to data. The MVCC implementation separates the public interface from internal version management.

### Version Chain Structure
```
User View (Public Interface):
+----------------+
| Record         |
|  schema_       |
|  values_       |
+----------------+

Internal Storage (Version Chain):
VersionedRecord                  VersionedRecord                  VersionedRecord
+------------------+            +------------------+            +------------------+
| version: 3       |            | version: 2       |            | version: 1       |
| timestamp: 100   |            | timestamp: 90    |            | timestamp: 80    |
| txn_id: 1001     |            | txn_id: 1000     |            | txn_id: 999      |
| is_deleted: false|            | is_deleted: false|            | is_deleted: false|
| data_ --------+  |            | data_ --------+  |            | data_ --------+  |
| prev_version_-+--+----------->| prev_version_-+--+----------->| prev_version_-+->null
+------------------+            +------------------+            +------------------+
        |                               |                               |
        v                               v                               v
    Record(v3)                      Record(v2)                      Record(v1)
+----------------+              +----------------+              +----------------+
| schema_        |              | schema_        |              | schema_        |
| values_        |              | values_        |              | values_        |
+----------------+              +----------------+              +----------------+
```

### Storage Layout
```
MemTable with Version Chains:
+------------------+
| SkipList         |
|  key1 -> ver3 ---+---> ver2 ----> ver1
|  key2 -> ver2 ---+---> ver1
|  key3 -> ver1    |
+------------------+

SSTable with Versioned Blocks:
+------------------+
| Header           |
+------------------+
| Data Block 1     |
|  key1:ver3 ------+---> ver2 ----> ver1
|  key2:ver2 ------+---> ver1
+------------------+
| Data Block 2     |
|  key3:ver2 ------+---> ver1
|  key4:ver1       |
+------------------+
| Version Index    |
| key1:{v3,v2,v1}  |
| key2:{v2,v1}     |
| key3:{v2,v1}     |
| key4:{v1}        |
+------------------+
| Index Block      |
+------------------+
| Footer           |
+------------------+
```

### Record vs VersionedRecord

#### Record (Public Interface)
The `Record` class provides a clean, version-agnostic interface for users:
```cpp
class Record {
    std::shared_ptr<Schema> schema_;
    std::vector<std::optional<common::DataChunk>> values_;
    
    // Public methods for data access
    void Set<T>(size_t col_idx, const T& value);
    std::optional<T> Get<T>(size_t col_idx) const;
    void SetNull(size_t col_idx);
};
```

Key characteristics:
- Simple data container focused on value storage
- Schema-aware operations
- No version management complexity
- Clean public API for users

#### VersionedRecord (Internal Implementation)
The `VersionedRecord` class handles version management internally:
```cpp
class VersionedRecord {
    std::shared_ptr<Record> data_;        // Actual record data
    uint64_t version_;                    // Version number
    uint64_t timestamp_;                  // Creation timestamp
    uint64_t txn_id_;                     // Creating transaction
    bool is_deleted_;                     // Tombstone flag
    std::shared_ptr<VersionedRecord> prev_version_;  // Version chain
};
```

Key characteristics:
- Internal to the storage engine
- Maintains version chains
- Handles transaction visibility
- Supports time-travel queries
- Manages record lifecycle

### Benefits of Separation
1. **Clean API**
   - Users work with simple Record objects
   - Version management is transparent to users
   - No exposure of MVCC implementation details

2. **Flexibility**
   - Version management can be modified without affecting user code
   - Easy to add new MVCC features
   - Simplified testing and maintenance

3. **Performance**
   - Efficient version chain traversal
   - Optimized storage in MemTable and SSTable
   - Better memory usage for single-version scenarios

## Metadata Tracking

The metadata tracking system maintains a consistent view of the table's state, including SSTable files and their sizes. This is implemented through a Write-Ahead Log (WAL) based state machine.

### TableMetadataEntry

The `TableMetadataEntry` class represents metadata operations:

- **Operation Types**
  - CreateSSTable: Track new SSTable files
  - DeleteSSTable: Remove SSTable files
  - UpdateStats: Update size statistics
  - FlushMemTable: Track memtable flushes
  - RotateWAL: Track WAL rotations

- **Features**
  - Multi-file operation support
  - Atomic tracking of related file changes
  - Serialization/deserialization for durability

### TableMetadataStateMachine

The `TableMetadataStateMachine` class manages the table's metadata state:

- **State Management**
  - Tracks active SSTable files
  - Maintains total size statistics
  - Supports checkpointing for recovery

- **Key Features**
  - WAL-based durability
  - Crash recovery support
  - Atomic multi-file operations
  - Checkpoint-based state recovery

### Usage

```cpp
// Create metadata state machine
TableMetadataStateMachine state_machine(fs, "table_metadata");
state_machine.Open();

// Track SSTable creation
std::vector<FileInfo> files = {{"file1.sst", 1000}, {"file2.sst", 2000}};
TableMetadataEntry entry(MetadataOpType::CreateSSTable, files);
state_machine.Apply(entry.Serialize());

// Get current state
auto files = state_machine.GetSSTableFiles();
auto total_size = state_machine.GetTotalSize();
```

### Benefits

1. **Durability**
   - WAL-based operation logging
   - Checkpoint-based state persistence
   - Crash recovery support

2. **Consistency**
   - Atomic multi-file operations
   - State machine approach ensures consistency
   - Safe concurrent access

3. **Extensibility**
   - Support for future operation types
   - Foundation for compaction tracking
   - Flexible state management

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

5. **Iterator Support**
   - Forward iterator with STL compatibility
   - Range-based for loop support
   - Block-level iteration with automatic boundary handling
   - Efficient seeking to specific keys
   - Memory-efficient block loading
   - Thread-safe iteration
   - Key range validation
   - Support for concurrent iterators

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

// Iterator usage
// Method 1: Using range-based for loop
for (const auto& [key, value] : reader) {
    // Process key-value pair
}

// Method 2: Using explicit iterator
auto iter = reader.NewIterator();
for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    auto key = iter->key();
    auto value = iter->value();
}

// Method 3: Using seek
auto iter = reader.NewIterator();
iter->Seek("target_key");
if (iter->Valid()) {
    // Found first key >= target_key
}
```

## KvTable

The `KvTable` class provides a schema-agnostic key-value store interface that serves as the base storage engine. It manages:

1. **Core Storage Operations**
   - Raw byte storage using string keys and DataChunk values
   - No schema validation or column awareness
   - Thread-safe operations with mutex protection

2. **Storage Components**
   - MemTable for in-memory storage
   - Write-Ahead Log (WAL) for durability
   - SSTable management for persistent storage
   - Metadata state machine for tracking table state

3. **Key Features**
   - Thread-safe operations
   - Crash recovery support
   - Automatic MemTable flushing
   - WAL rotation
   - SSTable organization

## Table

The `Table` class inherits from `KvTable` to provide a schema-aware interface. It adds:

1. **Schema Management**
   - Schema validation on write operations
   - Type-safe column operations
   - Record serialization/deserialization

2. **Schema-Aware Operations**
   - Put: Insert or update records with schema validation
   - Get: Retrieve and deserialize records
   - Delete: Remove records
   - Column operations: Update/retrieve individual columns

3. **Type Safety**
   - Records must match the table's schema
   - Column operations validate column names and types
   - Safe conversion between Records and raw bytes

### Usage

```cpp
// Create a schema-aware table
auto schema = std::make_shared<Schema>(...);
Table table(schema, fs, "my_table");

// Schema-aware operations
auto record = std::make_unique<Record>(schema);
record->Set(0, value1);
record->Set(1, value2);
table.Put(key, std::move(record));

// Column operations
table.UpdateColumn(key, "column_name", new_value);

// Get with automatic deserialization
auto result = table.Get(key);
if (result.ok()) {
    auto record = std::move(result).value();
    auto value = record->Get<Type>(column_index);
}
```

## Testing

The implementation includes comprehensive tests covering:
- Basic operations
- Data integrity
- Filter block functionality
- Block builder operations
- Error handling
- Edge cases