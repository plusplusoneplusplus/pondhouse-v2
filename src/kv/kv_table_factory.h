#pragma once

#include <memory>
#include <string>

#include "common/append_only_fs.h"
#include "kv/basic_kv_table.h"
#include "kv/i_kv_table.h"

namespace pond::kv {

/**
 * Factory class for creating different KvTable implementations.
 */
class KvTableFactory {
public:
    /**
     * Create a KvTable implementation.
     *
     * @param fs The filesystem to use
     * @param table_name The name of the table
     * @param max_wal_size The maximum WAL size
     * @return A shared pointer to an IKvTable implementation
     */
    static std::shared_ptr<IKvTable> Create(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                            const std::string& table_name,
                                            std::size_t max_wal_size = DEFAULT_WAL_SIZE) {
        return std::make_shared<BasicKvTable>(std::move(fs), table_name, max_wal_size);
    }

    /**
     * Create a specific KvTable implementation by type.
     *
     * @param type The type of KvTable to create ("basic" for BasicKvTable)
     * @param fs The filesystem to use
     * @param table_name The name of the table
     * @param max_wal_size The maximum WAL size
     * @return A shared pointer to an IKvTable implementation
     */
    static std::shared_ptr<IKvTable> CreateByType(const std::string& type,
                                                  std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                  const std::string& table_name,
                                                  std::size_t max_wal_size = DEFAULT_WAL_SIZE) {
        if (type == "basic") {
            return std::make_shared<BasicKvTable>(std::move(fs), table_name, max_wal_size);
        }

        // Default to BasicKvTable
        return std::make_shared<BasicKvTable>(std::move(fs), table_name, max_wal_size);
    }
};

}  // namespace pond::kv