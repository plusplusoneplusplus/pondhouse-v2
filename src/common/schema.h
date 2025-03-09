#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/column_type.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/serializable.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace pond::common {

enum class Nullability {
    NULLABLE,
    NOT_NULL,
};

class ColumnSchema : public ISerializable {
public:
    std::string name;
    ColumnType type;
    Nullability nullability;

    ColumnSchema(std::string name_, ColumnType type_, Nullability nullability_ = Nullability::NOT_NULL)
        : name(std::move(name_)), type(type_), nullability(nullability_) {}

    bool operator==(const ColumnSchema& other) const {
        return name == other.name && type == other.type && nullability == other.nullability;
    }

    DataChunk Serialize() const override {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        Serialize(writer);
        return DataChunk::FromString(buffer.GetString());
    }

    void Serialize(DataChunk& chunk) const override {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        Serialize(writer);
        chunk = DataChunk::FromString(buffer.GetString());
    }

    bool Deserialize(const DataChunk& chunk) override {
        rapidjson::Document doc;
        std::string json_str(reinterpret_cast<const char*>(chunk.Data()), chunk.Size());
        if (doc.Parse(json_str.c_str()).HasParseError()) {
            return false;
        }

        auto result = Deserialize(doc);
        if (!result.ok()) {
            return false;
        }

        auto column = std::move(result).value();
        name = std::move(column.name);
        type = column.type;
        nullability = column.nullability;
        return true;
    }

    void Serialize(rapidjson::Writer<rapidjson::StringBuffer>& writer) const {
        writer.StartObject();
        writer.Key("name");
        writer.String(name.c_str());
        writer.Key("type");
        writer.Int(static_cast<int>(type));
        writer.Key("nullability");
        writer.Int(static_cast<int>(nullability));
        writer.EndObject();
    }

    static Result<ColumnSchema> Deserialize(const rapidjson::Value& value) {
        if (!value.IsObject()) {
            return Result<ColumnSchema>::failure(ErrorCode::InvalidArgument, "Column schema must be an object");
        }

        if (!value.HasMember("name") || !value["name"].IsString()) {
            return Result<ColumnSchema>::failure(ErrorCode::InvalidArgument, "Column name must be a string");
        }

        if (!value.HasMember("type") || !value["type"].IsInt()) {
            return Result<ColumnSchema>::failure(ErrorCode::InvalidArgument, "Column type must be an integer");
        }

        if (!value.HasMember("nullability") || !value["nullability"].IsInt()) {
            return Result<ColumnSchema>::failure(ErrorCode::InvalidArgument, "Nullability must be an integer");
        }

        return Result<ColumnSchema>::success(ColumnSchema(value["name"].GetString(),
                                                          static_cast<ColumnType>(value["type"].GetInt()),
                                                          static_cast<Nullability>(value["nullability"].GetInt())));
    }

    std::string ToString() const {
        std::stringstream ss;
        ss << "ColumnSchema(" << name << ", " << ColumnTypeToString(type) << ", "
           << (nullability == Nullability::NULLABLE ? "NULLABLE" : "NOT NULL") << ")";
        return ss.str();
    }
};
class Schema : public ISerializable {
public:
    explicit Schema(std::vector<ColumnSchema> columns) : columns_(std::move(columns)) {
        for (size_t i = 0; i < columns_.size(); i++) {
            column_indices_[columns_[i].name] = i;
        }
    }

    bool operator==(const Schema& other) const {
        return column_indices_ == other.column_indices_ && columns_ == other.columns_;
    }

    Schema() = default;

    // TODO: remove these methods and replace with FieldCount, Fields, Empty
    const std::vector<ColumnSchema>& Columns() const { return columns_; }
    size_t NumColumns() const { return columns_.size(); }
    bool Empty() const { return columns_.empty(); }

    size_t FieldCount() const { return columns_.size(); }
    const std::vector<ColumnSchema>& Fields() const { return columns_; }

    int GetColumnIndex(const std::string& name) const {
        auto it = column_indices_.find(name);
        return it == column_indices_.end() ? -1 : static_cast<int>(it->second);
    }

    // Add a field to the schema
    void AddField(const std::string& name, ColumnType type, Nullability nullability = Nullability::NOT_NULL) {
        columns_.emplace_back(name, type, nullability);
        column_indices_[name] = columns_.size() - 1;
    }

    // Add a field with nullability specified as bool
    void AddField(const std::string& name, ColumnType type, bool nullable) {
        AddField(name, type, nullable ? Nullability::NULLABLE : Nullability::NOT_NULL);
    }

    // Add a field with nullability and default value
    void AddField(const std::string& name, ColumnType type, bool nullable, const DataChunk& default_value) {
        // Store default value in the future if needed
        AddField(name, type, nullable ? Nullability::NULLABLE : Nullability::NOT_NULL);
    }

    bool HasField(const std::string& name) const { return column_indices_.find(name) != column_indices_.end(); }

    const ColumnSchema& GetColumn(const std::string& name) const {
        auto it = column_indices_.find(name);
        if (it == column_indices_.end()) {
            throw std::runtime_error("Column not found: " + name);
        }
        return columns_[it->second];
    }

    std::vector<std::string> GetColumnNames() const {
        std::vector<std::string> names;
        for (const auto& column : columns_) {
            names.push_back(column.name);
        }
        return names;
    }

    DataChunk Serialize() const override {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

        writer.StartObject();
        writer.Key("columns");
        writer.StartArray();
        for (const auto& column : columns_) {
            column.Serialize(writer);
        }
        writer.EndArray();
        writer.EndObject();

        return DataChunk(reinterpret_cast<const uint8_t*>(buffer.GetString()), buffer.GetSize());
    }

    void Serialize(DataChunk& chunk) const override {
        auto serialized = Serialize();
        // Replace Append with assignment since DataChunk doesn't have Append
        chunk = serialized;
    }

    bool Deserialize(const DataChunk& data) override {
        rapidjson::Document doc;
        doc.Parse(reinterpret_cast<const char*>(data.Data()), data.Size());

        if (doc.HasParseError()) {
            return false;
        }

        if (!doc.IsObject() || !doc.HasMember("columns") || !doc["columns"].IsArray()) {
            return false;
        }

        std::vector<ColumnSchema> columns;
        const auto& columnsArray = doc["columns"];
        for (rapidjson::SizeType i = 0; i < columnsArray.Size(); i++) {
            auto column_result = ColumnSchema::Deserialize(columnsArray[i]);
            if (!column_result.ok()) {
                return false;
            }
            columns.push_back(std::move(column_result).value());
        }

        columns_ = std::move(columns);
        column_indices_.clear();
        for (size_t i = 0; i < columns_.size(); i++) {
            column_indices_[columns_[i].name] = i;
        }
        return true;
    }

    std::string ToString() const {
        std::stringstream ss;
        ss << "Schema(";
        for (const auto& column : columns_) {
            ss << column.ToString() << ", ";
        }
        ss << ")";
        return ss.str();
    }

private:
    std::vector<ColumnSchema> columns_;
    std::unordered_map<std::string, size_t> column_indices_;
};

// Builder class for Schema to simplify schema creation
class SchemaBuilder {
public:
    SchemaBuilder() : schema_(std::make_shared<Schema>()) {}

    // Add a field to the schema
    SchemaBuilder& AddField(const std::string& name, ColumnType type) {
        schema_->AddField(name, type);
        return *this;
    }

    // Add a field with nullability
    SchemaBuilder& AddField(const std::string& name, ColumnType type, bool nullable) {
        schema_->AddField(name, type, nullable);
        return *this;
    }

    // Add a field with nullability and default value
    SchemaBuilder& AddField(const std::string& name, ColumnType type, bool nullable, const DataChunk& default_value) {
        schema_->AddField(name, type, nullable, default_value);
        return *this;
    }

    // Build and return the schema
    std::shared_ptr<Schema> Build() const { return schema_; }

private:
    std::shared_ptr<Schema> schema_;
};

// Static method to create a SchemaBuilder
inline SchemaBuilder CreateSchemaBuilder() {
    return SchemaBuilder();
}

}  // namespace pond::common