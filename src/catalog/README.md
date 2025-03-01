# Catalog

The catalog component is responsible for managing the metadata of tables in the database.

Inspired by the [Apache Iceberg](https://iceberg.apache.org/) project. The catalog interface exposes following
operations:

- Create a table
- Load a table's metadata
- Drop a table
- Rename a table
- Update table properties
- Commit the changes to a table's metadata

The implementation mostly follows the spec defined in [Iceberg](https://iceberg.apache.org/spec/).

## Table

A logical table is defined as a collection of entities, with schema (fixed or can be evolved) and partition spec (fixed or can be lazily evaluated). The table only has a lastest version (called table metadata), while it may have multiple old version snapshots. 

### Table Metadata

Table metadata has following properties:

- Schema: current schema of the table
- Partition spec: current partition spec of the table
- Properties: table properties, which are used for storing metadata about the table, such as format, compression, etc. 
- Snapshots: list of snapshots that represent the history of the table.

#### Schema

Schema is a collection of fields. Each field has an internal id, a name, type, and nullable flag.
See `common::Schema` for more details.

#### PartitionSpec

PartitionSpec is a collection of partition fields. Each partition field has a source field, a name, a transform, and a
transform parameter.

supported transforms:

- IDENTITY
- YEAR
- MONTH
- DAY
- HOUR
- BUCKET
- TRUNCATE

#### Table Properties

Table properties are used for storing metadata about the table, such as format, compression, etc. For example,
`write.format.default` property can be used to specify the default format for the table.

#### Snapshot

Snapshot is a collection of changes to the table. Each snapshot has a snapshot id, a timestamp, an operation, and a
summary.

#### DataFile

DataFile is a collection of data blocks. Each data block has a path, a file size, a record count, and a partition values.

# Example

1. Metadata:

```json
{
  "format-version": 2,
  "table-uuid": "12345678-90ab-cdef-1234-567890abcdef",
  "location": "s3://example-bucket/iceberg/table/",
  "last-sequence-number": 3,
  "last-updated-ms": 1672345600000,
  "current-snapshot-id": 9876543210,
  "snapshots": [
    {
      "snapshot-id": 9876543210,
      "timestamp-ms": 1672345600000,
      "summary": {
        "operation": "append",
        "added-data-files": 3,
        "added-records": 1500,
        "changed-partition-count": 2
      },
      "manifest-list": "s3://example-bucket/iceberg/table/metadata/snap-9876543210.avro"
    }
  ],
  "partition-specs": [
    {
      "spec-id": 1,
      "fields": [
        {"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"},
        {"source-id": 3, "field-id": 1001, "name": "date", "transform": "day"}
      ]
    }
  ],
  "schema": {
    "type": "struct",
    "fields": [
      {"name": "id", "type": "long", "nullable": false},
      {"name": "name", "type": "string", "nullable": false},
      {"name": "region", "type": "string", "nullable": false},
      {"name": "date", "type": "timestamp", "nullable": false}
    ]
  }
}
```

2. Snapshot

```json
{
  "snapshot-id": 9876543210,
  "timestamp-ms": 1672345600000,
  "operation": "append",
  "manifest-list": "s3://example-bucket/iceberg/table/metadata/manifest-list.avro"
}
```

3. Manifest List

```json
[
  {
    "manifest-path": "s3://example-bucket/iceberg/table/metadata/manifest1.avro",
    "manifest-length": 1024,
    "partition-summaries": [
      {
        "contains-null": false,
        "lower-bound": {"region": "us-east", "date": "2023-01-01"},
        "upper-bound": {"region": "us-east", "date": "2023-01-31"}
      }
    ]
  },
  {
    "manifest-path": "s3://example-bucket/iceberg/table/metadata/manifest2.avro",
    "manifest-length": 2048,
    "partition-summaries": [
      {
        "contains-null": false,
        "lower-bound": {"region": "us-west", "date": "2023-02-01"},
        "upper-bound": {"region": "us-west", "date": "2023-02-28"}
      }
    ]
  }
]
```

4. Manifest

```json
{
  "added-data-files": [
    {
      "file-path": "s3://example-bucket/iceberg/table/data/file1.parquet",
      "file-format": "PARQUET",
      "partition-values": {"region": "us-east", "date": "2023-01-01"},
      "record-count": 500,
      "file-size-in-bytes": 1048576
    },
    {
      "file-path": "s3://example-bucket/iceberg/table/data/file2.parquet",
      "file-format": "PARQUET",
      "partition-values": {"region": "us-east", "date": "2023-01-15"},
      "record-count": 800,
      "file-size-in-bytes": 2048576
    }
  ],
  "existing-data-files": [],
  "deleted-data-files": []
}
```

# KVCatalog Design

The KVCatalog is an implementation of a metadata catalog system using key-value tables for storage. It provides transactional metadata management with snapshot isolation and schema evolution capabilities.

## Table Structure

The catalog uses three key-value tables for metadata storage:

1. **__tables**: Stores table metadata and current state
   ```
   Key: table_name
   Value: Record containing:
     - table_name: String (primary key)
     - table_uuid: String (unique identifier)
     - format_version: Int32 (schema version)
     - location: String (data location)
     - current_snapshot_id: Int64 (latest snapshot ID)
     - last_updated_time: Int64 (timestamp)
     - properties: Binary (serialized map)
     - schema: Binary (serialized schema)
     - partition_specs: Binary (serialized specs)
   ```

2. **__snapshots**: Stores snapshot history
   ```
   Key: "{table_name}/{snapshot_id}"
   Value: Record containing:
     - table_name: String
     - snapshot_id: Int64
     - parent_snapshot_id: Int64
     - timestamp_ms: Int64
     - operation: String (e.g., "CREATE", "APPEND")
     - manifest_list: String (path)
     - summary: Binary (serialized map)
   ```

3. **__files**: Stores data file metadata
   ```
   Key: "{table_name}/{snapshot_id}/{file_path}"
   Value: Record containing:
     - table_name: String
     - snapshot_id: Int64
     - file_path: String
     - file_format: String
     - record_count: Int64
     - file_size_bytes: Int64
     - partition_values: Binary (serialized map)
   ```

### Design Notes

1. **Single Source of Truth**: The current snapshot ID is stored directly in the table metadata record in `__tables`, eliminating the need for a separate current pointer table. This simplifies the design and reduces potential inconsistencies.

2. **Atomic Operations**: Table operations (create, update, rename) are protected by locks and use optimistic concurrency control by verifying the current snapshot ID hasn't changed during the transaction.

3. **Time Travel**: Historical versions can be accessed using snapshot IDs, which are stored in the `__snapshots` table. Each snapshot maintains a link to its parent, creating a chain of table states.

4. **Metadata Evolution**: Schema and partition specs are versioned within the table metadata, allowing for schema evolution while maintaining backward compatibility.

## Serialization

The catalog uses JSON format for all data serialization, providing consistency and flexibility across different data types:

1. **Properties**
   ```json
   {
     "key1": "value1",
     "key2": "value2"
   }
   ```

2. **Partition Values**
   ```json
   {
     "year": "2024",
     "month": "03",
     "region": "us-west"
   }
   ```

3. **Partition Specs**
   ```json
   [
     {
       "spec_id": 1,
       "fields": [
         {
           "source_id": 1,
           "field_id": 1000,
           "name": "year",
           "transform": "identity"
         },
         {
           "source_id": 2,
           "field_id": 1001,
           "name": "month",
           "transform": "month",
           "transform_param": 1
         }
       ]
     }
   ]
   ```

4. **Snapshots**
   ```json
   [
     {
       "snapshot_id": 1,
       "timestamp_ms": 1648176000000,
       "operation": "append",
       "manifest_list": "metadata/manifests/snap-1.avro",
       "summary": {
         "added-files": "10",
         "total-records": "1000"
       }
     }
   ]
   ```

5. **Data Files**
   ```json
   [
     {
       "file_path": "data/part-00000.parquet",
       "format": "parquet",
       "record_count": 1000,
       "file_size_bytes": 1048576,
       "partition_values": {
         "year": "2024",
         "month": "03"
       }
     }
   ]
   ```

This consistent use of JSON format across all serialization provides:
- Better readability and debugging
- Schema evolution support
- Standard parsing and error handling
- Easy integration with other tools and systems

