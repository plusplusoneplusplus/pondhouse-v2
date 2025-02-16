#pragma once

#include <atomic>
#include <chrono>
#include <unordered_map>
#include <vector>

#include "common/time.h"

namespace pond::kv {

// Forward declarations
class CompactionPhase {
public:
    enum class Phase { FileSelection, Reading, Merging, Writing, Complete, Failed };

    CompactionPhase() : phase_(Phase::FileSelection), start_time_(pond::common::now()) {}

    void UpdatePhase(Phase new_phase) {
        phase_ = new_phase;
        phase_start_times_[new_phase] = pond::common::now();
    }

    Phase GetCurrentPhase() const { return phase_; }

    pond::common::TimeInterval GetPhaseDuration(Phase phase) const {
        auto it = phase_start_times_.find(phase);
        if (it == phase_start_times_.end())
            return 0;

        auto end_time = (phase == phase_) ? pond::common::now()
                                          : phase_start_times_.at(static_cast<Phase>(static_cast<int>(phase) + 1));

        return pond::common::GetTimeInterval(it->second, end_time);
    }

private:
    Phase phase_;
    pond::common::Timestamp start_time_;
    std::unordered_map<Phase, pond::common::Timestamp> phase_start_times_;
};

struct CompactionJobMetrics {
    size_t job_id{0};
    size_t source_level{0};
    size_t target_level{0};
    size_t input_files{0};
    size_t output_files{0};
    size_t input_bytes{0};
    size_t output_bytes{0};
    size_t keys_processed{0};
    CompactionPhase phase;
    pond::common::Timestamp start_time{pond::common::now()};
    pond::common::Timestamp end_time{};
    bool is_completed{false};
    std::string error_message;

    // Default constructor
    CompactionJobMetrics() = default;

    // Constructor with parameters
    CompactionJobMetrics(size_t id, size_t src_level, size_t tgt_level)
        : job_id(id), source_level(src_level), target_level(tgt_level), start_time(pond::common::now()) {}

    // Copy constructor
    CompactionJobMetrics(const CompactionJobMetrics&) = default;
    // Move constructor
    CompactionJobMetrics(CompactionJobMetrics&&) noexcept = default;
    // Copy assignment
    CompactionJobMetrics& operator=(const CompactionJobMetrics&) = default;
    // Move assignment
    CompactionJobMetrics& operator=(CompactionJobMetrics&&) noexcept = default;
};

class CompactionMetrics {
public:
    // Compaction Statistics
    std::atomic<size_t> total_compactions_{0};
    std::atomic<size_t> failed_compactions_{0};
    std::atomic<size_t> total_bytes_compacted_{0};
    std::atomic<size_t> total_files_compacted_{0};
    std::vector<std::atomic<size_t>> compactions_per_level_;
    std::atomic<size_t> compaction_queue_length_{0};

    // Write Amplification Metrics
    std::atomic<size_t> total_bytes_written_{0};
    std::atomic<size_t> total_bytes_read_{0};
    std::vector<std::atomic<size_t>> bytes_written_per_level_;

    // Latency Metrics
    std::vector<pond::common::TimeInterval> compaction_durations_;
    std::unordered_map<CompactionPhase::Phase, pond::common::TimeInterval> phase_durations_;

    // Resource Usage
    std::atomic<size_t> peak_memory_usage_{0};
    std::atomic<size_t> current_memory_usage_{0};
    std::atomic<size_t> disk_read_bytes_{0};
    std::atomic<size_t> disk_write_bytes_{0};

    // Level-specific Metrics
    std::vector<std::atomic<size_t>> level_sizes_;
    std::vector<std::atomic<size_t>> files_per_level_;

    // Health Metrics
    std::atomic<size_t> l0_stall_count_{0};
    std::atomic<size_t> compaction_backlog_{0};
    std::atomic<size_t> error_count_{0};

    // Active compaction tracking
    std::unordered_map<size_t, CompactionJobMetrics> active_compactions_;

public:
    CompactionMetrics(size_t max_levels = 7)
        : compactions_per_level_(max_levels),
          bytes_written_per_level_(max_levels),
          level_sizes_(max_levels),
          files_per_level_(max_levels) {}

    void Reset() {
        total_compactions_ = 0;
        failed_compactions_ = 0;
        total_bytes_compacted_ = 0;
        total_files_compacted_ = 0;
        for (auto& level_count : compactions_per_level_) {
            level_count = 0;
        }
        bytes_written_per_level_.clear();
        level_sizes_.clear();
        files_per_level_.clear();
        compaction_durations_.clear();
        phase_durations_.clear();
        active_compactions_.clear();
        total_bytes_written_ = 0;
        total_bytes_read_ = 0;
        peak_memory_usage_ = 0;
        current_memory_usage_ = 0;
        disk_read_bytes_ = 0;
        disk_write_bytes_ = 0;
        l0_stall_count_ = 0;
        compaction_backlog_ = 0;
        error_count_ = 0;
        compaction_queue_length_ = 0;
    }

    // Record start of a compaction job
    CompactionJobMetrics& StartCompactionJob(size_t job_id, size_t source_level, size_t target_level) {
        CompactionJobMetrics metrics(job_id, source_level, target_level);
        auto [it, inserted] = active_compactions_.emplace(job_id, std::move(metrics));
        compaction_queue_length_++;
        return it->second;
    }

    // Record completion of a compaction job
    void CompleteCompactionJob(size_t job_id, bool success) {
        auto it = active_compactions_.find(job_id);
        if (it == active_compactions_.end())
            return;

        auto& job = it->second;
        job.is_completed = true;
        job.end_time = pond::common::now();

        if (success) {
            total_compactions_++;
            compactions_per_level_[job.source_level]++;
            total_bytes_compacted_ += job.input_bytes;
            total_files_compacted_ += job.input_files;

            // Update write amplification metrics
            total_bytes_written_ += job.output_bytes;
            total_bytes_read_ += job.input_bytes;
            bytes_written_per_level_[job.target_level] += job.output_bytes;
        } else {
            failed_compactions_++;
            error_count_++;
        }

        compaction_queue_length_--;
    }

    // Update resource usage
    void UpdateResourceUsage(size_t memory_usage, size_t disk_read, size_t disk_write) {
        current_memory_usage_.store(memory_usage);
        peak_memory_usage_.store(std::max(peak_memory_usage_.load(), memory_usage));
        disk_read_bytes_ += disk_read;
        disk_write_bytes_ += disk_write;
    }

    // Update level metrics
    void UpdateLevelMetrics(size_t level, size_t size, size_t file_count) {
        if (level < level_sizes_.size()) {
            level_sizes_[level].store(size);
            files_per_level_[level].store(file_count);
        }
    }

    // Calculate write amplification
    double GetWriteAmplification() const {
        size_t bytes_written = total_bytes_written_.load();
        size_t bytes_read = total_bytes_read_.load();
        return bytes_read > 0 ? static_cast<double>(bytes_written) / bytes_read : 0.0;
    }

    // Get average compaction duration
    pond::common::TimeInterval GetAverageCompactionDuration() const {
        if (compaction_durations_.empty())
            return 0;

        pond::common::TimeInterval total(0);
        for (const auto& duration : compaction_durations_) {
            total += duration;
        }
        return total / compaction_durations_.size();
    }
};

}  // namespace pond::kv