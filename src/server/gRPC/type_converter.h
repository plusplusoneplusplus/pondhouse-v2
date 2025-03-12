#pragma once

#include "catalog/catalog.h"
#include "common/schema.h"
#include "pond_service_impl.h"

namespace pond::server {

class TypeConverter {
public:
    /**
     * Convert a catalog TableMetadata to protocol buffer TableMetadataInfo
     */
    static pond::proto::TableMetadataInfo ConvertToTableMetadataInfo(
        const pond::catalog::TableMetadata& table_metadata) {
        pond::proto::TableMetadataInfo pb_metadata;

        // Set basic metadata
        pb_metadata.set_name(table_metadata.name);
        pb_metadata.set_location(table_metadata.location);
        pb_metadata.set_last_updated_time(table_metadata.last_updated_time);

        // Set schema columns
        if (table_metadata.schema) {
            for (const auto& column : table_metadata.schema->Columns()) {
                auto* col_info = pb_metadata.add_columns();
                col_info->set_name(column.name);
                col_info->set_type(pond::common::ColumnTypeToString(column.type));
            }
        }

        // Set partition columns and spec
        if (!table_metadata.partition_specs.empty()) {
            const auto& partition_spec = table_metadata.partition_specs.back();  // Use the latest spec
            
            // Add partition columns
            for (const auto& field : partition_spec.fields) {
                pb_metadata.add_partition_columns(field.name);
            }

            // Set partition spec
            auto* pb_spec = pb_metadata.mutable_partition_spec();
            for (const auto& field : partition_spec.fields) {
                auto* pb_field = pb_spec->add_fields();
                pb_field->set_source_id(field.source_id);
                pb_field->set_field_id(field.field_id);
                pb_field->set_name(field.name);
                pb_field->set_transform(pond::catalog::TransformToString(field.transform));
                if (field.transform_param) {
                    pb_field->set_transform_param(std::to_string(*field.transform_param));
                }
            }
        }

        // Set properties
        for (const auto& [key, value] : table_metadata.properties) {
            (*pb_metadata.mutable_properties())[key] = value;
        }

        return pb_metadata;
    }

    /**
     * Add data files to the TableMetadataInfo from a list of catalog DataFiles
     */
    static void AddDataFilesToTableMetadataInfo(pond::proto::TableMetadataInfo* pb_metadata,
                                                const std::vector<pond::catalog::DataFile>& data_files) {
        for (const auto& data_file : data_files) {
            auto* file_info = pb_metadata->add_data_files();
            file_info->set_path(data_file.file_path);
            file_info->set_content_length(data_file.file_size_bytes);
            file_info->set_record_count(data_file.record_count);

            // Add partition values
            for (const auto& [key, value] : data_file.partition_values) {
                (*file_info->mutable_partition_values())[key] = value;
            }
        }
    }
};

}  // namespace pond::server
