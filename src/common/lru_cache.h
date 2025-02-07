#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "common/result.h"
#include "common/time.h"

namespace pond::common {

struct LRUCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t evictions{0};
    size_t current_memory_bytes{0};

    double hit_rate() const {
        const uint64_t total = hits + misses;
        return total == 0 ? 0.0 : static_cast<double>(hits) / total;
    }
};

template <typename K, typename V, typename Hash = std::hash<K>>
class LRUCache {
public:
    struct CacheEntry {
        V value;
        Timestamp last_access;
        uint64_t access_count{0};
        size_t memory_bytes{0};
    };

    using MemoryCalculator = std::function<size_t(const V&)>;

    explicit LRUCache(
        size_t max_memory_bytes, MemoryCalculator memory_calculator = [](const V&) { return 1; })
        : max_memory_bytes_(max_memory_bytes), memory_calculator_(memory_calculator) {}

    Result<std::optional<V>> Get(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = cache_.find(key);
        if (it == cache_.end()) {
            stats_.misses++;
            return Result<std::optional<V>>(std::nullopt);
        }

        // Move to front of LRU list
        lru_list_.erase(it->second.first);
        lru_list_.push_front(key);
        it->second.first = lru_list_.begin();

        // Update entry stats
        it->second.second.last_access = now();
        it->second.second.access_count++;
        stats_.hits++;

        return Result<std::optional<V>>(it->second.second.value);
    }

    Result<void> Put(const K& key, V value) {
        std::lock_guard<std::mutex> lock(mutex_);

        size_t value_size = memory_calculator_(value);

        // Handle update case
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            stats_.current_memory_bytes -= it->second.second.memory_bytes;
            lru_list_.erase(it->second.first);
            cache_.erase(it);  // Remove old entry completely
        }

        // Ensure we have space
        while (!lru_list_.empty() && (stats_.current_memory_bytes + value_size > max_memory_bytes_)) {
            EvictLRU();
        }

        // If the single item is too large, return error
        if (value_size > max_memory_bytes_) {
            return Result<void>(Error(ErrorCode::InvalidArgument, "Cache entry size exceeds maximum cache size"));
        }

        // Insert new entry
        lru_list_.push_front(key);
        CacheEntry entry{
            .value = std::move(value), .last_access = now(), .access_count = 0, .memory_bytes = value_size};
        cache_.emplace(key, std::make_pair(lru_list_.begin(), std::move(entry)));
        stats_.current_memory_bytes += value_size;

        return Result<void>();
    }

    Result<void> Remove(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            stats_.current_memory_bytes -= it->second.second.memory_bytes;
            lru_list_.erase(it->second.first);
            cache_.erase(it);
        }

        return Result<void>();
    }

    Result<void> Clear() {
        std::lock_guard<std::mutex> lock(mutex_);

        lru_list_.clear();
        cache_.clear();
        stats_ = LRUCacheStats{};

        return Result<void>();
    }

    LRUCacheStats GetStats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return stats_;
    }

private:
    void EvictLRU() {
        if (lru_list_.empty())
            return;

        auto lru_key = lru_list_.back();
        auto it = cache_.find(lru_key);
        if (it != cache_.end()) {
            stats_.current_memory_bytes -= it->second.second.memory_bytes;
            cache_.erase(it);
            stats_.evictions++;
        }
        lru_list_.pop_back();
    }

    const size_t max_memory_bytes_;
    const MemoryCalculator memory_calculator_;

    std::list<K> lru_list_;
    std::unordered_map<K, std::pair<typename std::list<K>::iterator, CacheEntry>, Hash> cache_;

    mutable std::mutex mutex_;
    LRUCacheStats stats_;
};

}  // namespace pond::common
