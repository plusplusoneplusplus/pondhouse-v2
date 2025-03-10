#pragma once

#include <string>

namespace pond::common {

enum class ErrorCode {
    // General errors
    Success = 0,
    Failure = 1,
    Unknown = 2,
    InternalError = 3,
    InvalidArgument = 4,
    InvalidOperation = 5,
    UnsupportedOperation = 6,
    NotImplemented = 7,
    NotFound = 8,
    DeserializationError = 9,
    EndOfStream = 10,
    Timeout = 11,
    NullValue = 12,

    // File system errors
    FileNotFound = 100,
    FileAlreadyExists = 101,
    FileCreationError = 102,
    FileReadFailed = 103,
    FileWriteFailed = 104,
    FileOpenFailed = 105,
    FileCloseFailed = 106,
    FileSeekFailed = 107,
    FileTruncateFailed = 108,
    FileCorrupted = 109,
    InvalidHandle = 110,
    FileDeleteFailed = 111,
    FileCreateFailed = 112,
    FileMoveFailed = 113,
    FileAccessFailed = 114,

    // Directory errors
    DirectoryNotFound = 120,
    DirectoryAlreadyExists = 121,
    DirectoryNotEmpty = 122,
    DirectoryCreateFailed = 123,
    DirectoryDeleteFailed = 124,
    DirectoryMoveFailed = 125,
    DirectoryAccessFailed = 126,
    NotADirectory = 127,

    // Parquet errors
    ParquetReadFailed = 200,
    ParquetWriteFailed = 201,
    ParquetSchemaError = 202,
    ParquetReaderNotInitialized = 203,
    ParquetWriterNotInitialized = 204,
    ParquetSchemaConversionError = 205,
    ParquetIncompatibleSchema = 206,
    ParquetUnsupportedType = 207,
    ParquetInvalidColumn = 208,
    ParquetInvalidPredicate = 209,
    ParquetInvalidData = 210,
    ParquetFileCreationError = 211,
    ParquetMemoryAllocationError = 212,
    ParquetBufferOverflow = 213,
    ParquetInvalidNullability = 214,

    // Data structure errors
    BloomFilterFull = 300,
    BloomFilterInvalid = 301,
    BTreeNodeTooSmall = 302,
    BTreeNodeFull = 303,
    BTreeInvalid = 304,
    BTreeNodeCorrupted = 305,
    SSTableEmpty = 306,
    SSTableInvalid = 307,
    SSTableSizeLimitExceeded = 308,
    SSTableUnsupportedVersion = 309,
    SSTableInvalidMagicNumber = 310,
    SSTableReadError = 311,
    WALInvalid = 312,
    WALWriteFailed = 313,
    WALReadFailed = 314,

    // Table errors
    TableNotOpen = 400,
    TableAlreadyOpen = 401,
    TableNotFound = 402,
    TableCorrupted = 403,
    TableFull = 404,
    KeyValueSizeTooLarge = 405,
    MemtableFull = 406,
    SSTableCreationFailed = 407,
    WALCreationFailed = 408,
    RecoveryFailed = 409,
    FlushFailed = 410,
    CreateTableFailed = 411,

    // Bloom filter errors
    BloomFilterInvalidDataSize = 500,
    BloomFilterInvalidMagicNumber = 501,
    BloomFilterUnsupportedVersion = 502,

    // Schema errors
    SchemaFieldNotFound = 600,
    SchemaMismatch = 601,

    // Predicate engine errors
    PredicateEngineInvalidExpression = 700,

    // Catalog errors
    TableNotFoundInCatalog = 800,
};

class Error {
public:
    Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}
    Error(ErrorCode code) : code_(code), message_("") {}

    ErrorCode code() const { return code_; }
    const std::string& message() const { return message_; }
    const char* c_str() const { return message_.c_str(); }

    std::string to_string() const;

private:
    ErrorCode code_;
    std::string message_;
};

}  // namespace pond::common