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
};

class Schema : public ISerializable {
public:
    explicit Schema(std::vector<ColumnSchema> columns) : columns_(std::move(columns)) {
        for (size_t i = 0; i < columns_.size(); i++) {
            column_indices_[columns_[i].name] = i;
        }
    }

    Schema() = default;

    const std::vector<ColumnSchema>& columns() const { return columns_; }
    size_t num_columns() const { return columns_.size(); }
    bool empty() const { return columns_.empty(); }

    int GetColumnIndex(const std::string& name) const {
        auto it = column_indices_.find(name);
        return it == column_indices_.end() ? -1 : static_cast<int>(it->second);
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
        chunk.Append(serialized.Data(), serialized.Size());
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

private:
    std::vector<ColumnSchema> columns_;
    std::unordered_map<std::string, size_t> column_indices_;
};

}  // namespace pond::common