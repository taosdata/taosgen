#pragma once
#include "MemoryPool.hpp"
#include <cstdint>
#include <cstddef>

struct BaseInsertData {
    enum class DataType {
        BASE,
        SQL,
        STMT,
        MSG
    };
    DataType type;

    int64_t start_time;
    int64_t end_time;
    size_t total_rows;

    BaseInsertData(MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances)
        : BaseInsertData(DataType::BASE, block, col_instances) {}

    BaseInsertData(DataType t, MemoryPool::MemoryBlock* block, const ColumnConfigInstanceVector& col_instances)
        : type(t),
          start_time(block->start_time),
          end_time(block->end_time),
          total_rows(block->total_rows),
          block_(block),
          col_instances_(col_instances) {}

    // Move constructor
    BaseInsertData(BaseInsertData&& other) noexcept
        : type(other.type),
          start_time(other.start_time),
          end_time(other.end_time),
          total_rows(other.total_rows),
          block_(other.block_),
          col_instances_(other.col_instances_) {
        other.block_ = nullptr;
        other.type = DataType::BASE;
    }

    virtual ~BaseInsertData() {
        if (block_) {
            block_->release();
            block_ = nullptr;
        }
    }

    // Disable copy
    BaseInsertData(const BaseInsertData&) = delete;
    BaseInsertData& operator=(const BaseInsertData&) = delete;
    BaseInsertData& operator=(BaseInsertData&&) = delete;

    size_t row_count() const noexcept {
        return block_ ? block_->total_rows : 0;
    }

    size_t column_count() const noexcept {
        return col_instances_.size();
    }

    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept {
        return block_ ? &block_->bindv_ : nullptr;
    }

    MemoryPool::MemoryBlock* get_block() const noexcept {
        return block_;
    }

    void reset_block() noexcept {
        block_ = nullptr;
    }

private:
    MemoryPool::MemoryBlock* block_;
    const ColumnConfigInstanceVector& col_instances_;
};