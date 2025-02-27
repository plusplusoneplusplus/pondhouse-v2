#include "catalog/types.h"

#include <stdexcept>
#include <unordered_map>

namespace pond::catalog {

namespace {
const std::unordered_map<Operation, std::string> kOperationToString = {{Operation::CREATE, "create"},
                                                                       {Operation::APPEND, "append"},
                                                                       {Operation::REPLACE, "replace"},
                                                                       {Operation::DELETE, "delete"},
                                                                       {Operation::OVERWRITE, "overwrite"}};

const std::unordered_map<std::string, Operation> kStringToOperation = {{"append", Operation::APPEND},
                                                                       {"replace", Operation::REPLACE},
                                                                       {"delete", Operation::DELETE},
                                                                       {"overwrite", Operation::OVERWRITE}};

const std::unordered_map<Transform, std::string> kTransformToString = {{Transform::IDENTITY, "identity"},
                                                                       {Transform::YEAR, "year"},
                                                                       {Transform::MONTH, "month"},
                                                                       {Transform::DAY, "day"},
                                                                       {Transform::HOUR, "hour"},
                                                                       {Transform::BUCKET, "bucket"},
                                                                       {Transform::TRUNCATE, "truncate"}};

const std::unordered_map<std::string, Transform> kStringToTransform = {{"identity", Transform::IDENTITY},
                                                                       {"year", Transform::YEAR},
                                                                       {"month", Transform::MONTH},
                                                                       {"day", Transform::DAY},
                                                                       {"hour", Transform::HOUR},
                                                                       {"bucket", Transform::BUCKET},
                                                                       {"truncate", Transform::TRUNCATE}};

const std::unordered_map<FileFormat, std::string> kFileFormatToString = {{FileFormat::PARQUET, "parquet"}};

const std::unordered_map<std::string, FileFormat> kStringToFileFormat = {{"parquet", FileFormat::PARQUET}};
}  // namespace

std::string OperationToString(Operation op) {
    auto it = kOperationToString.find(op);
    if (it == kOperationToString.end()) {
        throw std::invalid_argument("Unknown operation");
    }
    return it->second;
}

Operation OperationFromString(const std::string& str) {
    auto it = kStringToOperation.find(str);
    if (it == kStringToOperation.end()) {
        throw std::invalid_argument("Unknown operation string: " + str);
    }
    return it->second;
}

std::string TransformToString(Transform transform) {
    auto it = kTransformToString.find(transform);
    if (it == kTransformToString.end()) {
        throw std::invalid_argument("Unknown transform");
    }
    return it->second;
}

Transform TransformFromString(const std::string& str) {
    auto it = kStringToTransform.find(str);
    if (it == kStringToTransform.end()) {
        throw std::invalid_argument("Unknown transform string: " + str);
    }
    return it->second;
}

std::string FileFormatToString(FileFormat format) {
    auto it = kFileFormatToString.find(format);
    if (it == kFileFormatToString.end()) {
        throw std::invalid_argument("Unknown file format");
    }
    return it->second;
}

FileFormat FileFormatFromString(const std::string& str) {
    auto it = kStringToFileFormat.find(str);
    if (it == kStringToFileFormat.end()) {
        throw std::invalid_argument("Unknown file format string: " + str);
    }
    return it->second;
}

}  // namespace pond::catalog