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
