#include "MemoryPool.hpp"
#include <iostream>
#include <cstring>
#include <new>
#include <limits>
#include <stdexcept>

constexpr size_t MEMORY_POOL_ALIGNMENT = 64;

inline size_t align_up(size_t x, size_t align) {
    return ((x + align - 1) / align) * align;
}

MemoryPool::MemoryPool(size_t block_count,
                       size_t max_tables_per_block,
                       size_t max_rows_per_table,
                       const ColumnConfigInstanceVector& col_instances,
                       bool tables_reuse_data,
                       size_t num_cached_blocks
                    )
    : max_tables_per_block_(max_tables_per_block),
      max_rows_per_table_(max_rows_per_table),
      col_instances_(col_instances),
      col_handlers_(ColumnConverter::create_handlers_for_columns(col_instances)),
      blocks_(block_count),
      free_queue_(block_count),
      tables_reuse_data_(tables_reuse_data),
      num_cached_blocks_(num_cached_blocks)
{
    size_t max_rows_per_block = max_tables_per_block * max_rows_per_table;
    if (tables_reuse_data) {
        max_rows_per_block = max_rows_per_table;
    }
    const size_t col_count = col_instances.size();

    // Total size for timestamp section
    timestamps_size_ = max_tables_per_block * max_rows_per_table * sizeof(int64_t);

    // Calculate total size for columns
    common_meta_size_ = 0;
    fixed_data_size_ = 0;
    var_meta_size_ = 0;
    var_data_size_ = 0;

    for (const auto& col_instance : col_instances) {
        // is_nulls
        common_meta_size_ += max_rows_per_block * sizeof(char);

        if (col_instance.config().is_var_length()) {
            // lengths + offsets
            var_meta_size_ += max_rows_per_block * (sizeof(int32_t) + sizeof(size_t));
            var_data_size_ += max_rows_per_block * col_instance.config().len.value();
        } else {
            fixed_data_size_ += max_rows_per_block * col_instance.config().get_fixed_type_size();
        }
    }

    // Total memory size
    total_cache_size_ = common_meta_size_ + fixed_data_size_ + var_meta_size_ + var_data_size_;
    total_cache_size_ = align_up(total_cache_size_, MEMORY_POOL_ALIGNMENT);

    // 根据是否缓存模式初始化
    if (num_cached_blocks_ > 0) {
        init_cache_units();
        init_cached_blocks();
    } else {
        init_normal_blocks();
    }

    // 将所有块加入空闲队列
    for (auto& block : blocks_) {
        free_queue_.enqueue(&block);
    }
}

MemoryPool::~MemoryPool() {
    for (auto& block : blocks_) {
        block.owning_pool = nullptr;
        block.release_data_chunk();
    }

    // 释放缓存单元
    for (auto& cache_unit : cache_units_) {
        if (cache_unit.data_chunk) {
            std::free(cache_unit.data_chunk);
            cache_unit.data_chunk = nullptr;
            cache_unit.data_chunk_size = 0;
        }
    }
}

void MemoryPool::init_cache_units() {
    cache_units_.resize(num_cached_blocks_);

    for (auto& cache_unit : cache_units_) {
        cache_unit.data_chunk = std::aligned_alloc(MEMORY_POOL_ALIGNMENT, total_cache_size_);
        cache_unit.data_chunk_size = total_cache_size_;

        if (!cache_unit.data_chunk) {
            throw std::bad_alloc();
        }

        // Initialize memory to zero
        std::memset(cache_unit.data_chunk, 0, cache_unit.data_chunk_size);
        char* current_ptr = static_cast<char*>(cache_unit.data_chunk);

        // Allocate memory for fixed-length column data
        cache_unit.fixed_data_base = current_ptr;
        current_ptr += fixed_data_size_;

        // Allocate memory for variable-length column metadata
        cache_unit.var_meta_base = current_ptr;
        current_ptr += var_meta_size_;

        // Allocate memory for variable-length column data
        cache_unit.var_data_base = current_ptr;
        current_ptr += var_data_size_;

        // Allocate memory for common metadata
        cache_unit.common_meta_base = current_ptr;
        current_ptr += common_meta_size_;
    }
}

void MemoryPool::init_normal_blocks() {
    size_t total_block_size = timestamps_size_ + total_cache_size_;
    total_block_size = align_up(total_block_size, MEMORY_POOL_ALIGNMENT);

    for (auto& block : blocks_) {
        block.owning_pool = this;

        // Allocate a single large memory block
        block.data_chunk = std::aligned_alloc(MEMORY_POOL_ALIGNMENT, total_block_size);
        block.data_chunk_size = total_block_size;

        if (!block.data_chunk) {
            throw std::bad_alloc();
        }

        // Initialize memory to zero
        std::memset(block.data_chunk, 0, total_block_size);
        char* current_ptr = static_cast<char*>(block.data_chunk);

        // Allocate memory for timestamps
        int64_t* timestamps_base = reinterpret_cast<int64_t*>(current_ptr);
        current_ptr += timestamps_size_;

        // Allocate memory for fixed-length column data
        void* fixed_data_base = current_ptr;
        current_ptr += fixed_data_size_;

        // Allocate memory for variable-length column metadata
        void* var_meta_base = current_ptr;
        current_ptr += var_meta_size_;

        // Allocate memory for variable-length column data
        char* var_data_base = current_ptr;
        current_ptr += var_data_size_;

        // Allocate memory for common metadata
        char* common_meta_base = current_ptr;
        current_ptr += common_meta_size_;

        // Initialize table structure
        block.tables.resize(max_tables_per_block_);

        // Initialize table metadata
        for (size_t i = 0; i < max_tables_per_block_; ++i) {
            auto& table = block.tables[i];
            table.max_rows = max_rows_per_table_;
            table.col_handlers_ptr = &col_handlers_;
            table.columns.resize(col_instances_.size());

            // Set timestamp pointer
            table.timestamps = timestamps_base + i * max_rows_per_table_;
        }

        // Set column pointers
        char* fixed_col_ptr = static_cast<char*>(fixed_data_base);
        char* var_meta_ptr = static_cast<char*>(var_meta_base);
        char* var_data_ptr = var_data_base;
        char* common_meta_ptr = common_meta_base;

        const size_t is_nulls_size = max_rows_per_table_ * sizeof(char);
        const size_t lengths_size = max_rows_per_table_ * sizeof(int32_t);
        const size_t offsets_size = max_rows_per_table_ * sizeof(size_t);

        for (size_t table_idx = 0; table_idx < max_tables_per_block_; ++table_idx) {
            for (size_t col_idx = 0; col_idx < col_instances_.size(); ++col_idx) {
                auto& col = block.tables[table_idx].columns[col_idx];
                const auto& config = col_instances_[col_idx].config();

                col.is_nulls = common_meta_ptr;
                common_meta_ptr += is_nulls_size;

                if (config.is_var_length()) {
                    col.is_fixed = false;
                    col.max_length = config.len.value();
                    col.element_size = 0;

                    // lengths array
                    col.lengths = reinterpret_cast<int32_t*>(var_meta_ptr);
                    var_meta_ptr += lengths_size;

                    // offsets array
                    col.var_offsets = reinterpret_cast<size_t*>(var_meta_ptr);
                    var_meta_ptr += offsets_size;

                    // var_data data area
                    const size_t col_data_size = max_rows_per_table_ * config.len.value();
                    col.var_data = var_data_ptr;
                    var_data_ptr += col_data_size;
                } else {
                    col.is_fixed = true;
                    col.element_size = config.get_fixed_type_size();
                    col.max_length = col.element_size;

                    // fixed_data data area
                    const size_t col_data_size = max_rows_per_table_ * col.element_size;
                    col.fixed_data = fixed_col_ptr;
                    fixed_col_ptr += col_data_size;
                }
            }

            if (tables_reuse_data_) {
                fixed_col_ptr = static_cast<char*>(fixed_data_base);
                var_meta_ptr = static_cast<char*>(var_meta_base);
                var_data_ptr = var_data_base;
                common_meta_ptr = common_meta_base;
            }
        }

        // Initialize bindv structure
        block.init_bindv();

        // reset block state
        block.reset();
    }
}

void MemoryPool::init_cached_blocks() {
    for (size_t block_idx = 0; block_idx < blocks_.size(); ++block_idx) {
        auto& block = blocks_[block_idx];
        block.owning_pool = this;

        // 分配时间戳内存
        block.data_chunk = std::aligned_alloc(MEMORY_POOL_ALIGNMENT, timestamps_size_);
        block.data_chunk_size = timestamps_size_;

        if (!block.data_chunk) {
            throw std::bad_alloc();
        }

        // Initialize memory to zero
        std::memset(block.data_chunk, 0, block.data_chunk_size);

        // 获取对应的缓存单元
        size_t cache_index = block_idx % num_cached_blocks_;
        auto& cache_unit = cache_units_[cache_index];

        // Initialize table structure
        block.tables.resize(max_tables_per_block_);

        // Initialize table metadata
        int64_t* timestamps_base = reinterpret_cast<int64_t*>(block.data_chunk);
        for (size_t i = 0; i < max_tables_per_block_; ++i) {
            auto& table = block.tables[i];
            table.max_rows = max_rows_per_table_;
            table.col_handlers_ptr = &col_handlers_;
            table.columns.resize(col_instances_.size());

            // Set timestamp pointer
            table.timestamps = timestamps_base + i * max_rows_per_table_;
        }

        // Set column pointers
        char* fixed_col_ptr = static_cast<char*>(cache_unit.fixed_data_base);
        char* var_meta_ptr = static_cast<char*>(cache_unit.var_meta_base);
        char* var_data_ptr = cache_unit.var_data_base;
        char* common_meta_ptr = cache_unit.common_meta_base;

        const size_t is_nulls_size = max_rows_per_table_ * sizeof(char);
        const size_t lengths_size = max_rows_per_table_ * sizeof(int32_t);
        const size_t offsets_size = max_rows_per_table_ * sizeof(size_t);

        for (size_t table_idx = 0; table_idx < max_tables_per_block_; ++table_idx) {
            for (size_t col_idx = 0; col_idx < col_instances_.size(); ++col_idx) {
                auto& col = block.tables[table_idx].columns[col_idx];
                const auto& config = col_instances_[col_idx].config();

                col.is_nulls = common_meta_ptr;
                common_meta_ptr += is_nulls_size;

                if (config.is_var_length()) {
                    col.is_fixed = false;
                    col.max_length = config.len.value();
                    col.element_size = 0;

                    // lengths array
                    col.lengths = reinterpret_cast<int32_t*>(var_meta_ptr);
                    var_meta_ptr += lengths_size;

                    // offsets array
                    col.var_offsets = reinterpret_cast<size_t*>(var_meta_ptr);
                    var_meta_ptr += offsets_size;

                    // var_data data area
                    const size_t col_data_size = max_rows_per_table_ * config.len.value();
                    col.var_data = var_data_ptr;
                    var_data_ptr += col_data_size;
                } else {
                    col.is_fixed = true;
                    col.element_size = config.get_fixed_type_size();
                    col.max_length = col.element_size;

                    // fixed_data data area
                    const size_t col_data_size = max_rows_per_table_ * col.element_size;
                    col.fixed_data = fixed_col_ptr;
                    fixed_col_ptr += col_data_size;
                }
            }

            if (tables_reuse_data_) {
                fixed_col_ptr = static_cast<char*>(cache_unit.fixed_data_base);
                var_meta_ptr = static_cast<char*>(cache_unit.var_meta_base);
                var_data_ptr = cache_unit.var_data_base;
                common_meta_ptr = cache_unit.common_meta_base;
            }
        }

        // Initialize bindv structure
        block.init_bindv();

        // reset block state
        block.reset();
    }
}

MemoryPool::MemoryBlock* MemoryPool::acquire_block(size_t sequence_num) {
    MemoryBlock* block = nullptr;
    free_queue_.wait_dequeue(block);

    block->in_use = true;
    block->reset();
    return block;
}

void MemoryPool::release_block(MemoryBlock* block) {
    if (block) {
        block->in_use = false;
        free_queue_.enqueue(block);
    }
}

MemoryPool::MemoryBlock* MemoryPool::convert_to_memory_block(MultiBatch&& batch) {
    if (batch.table_batches.empty() || batch.total_rows == 0) {
        return nullptr;
    }

    // 1. Get memory block from pool
    auto* block = acquire_block();
    // if (!block) {
    //     throw std::runtime_error("Failed to acquire memory block from pool");
    // }

    // 2. Set memory block metadata
    // block->reset();
    block->start_time = batch.start_time;
    block->end_time = batch.end_time;
    block->total_rows = batch.total_rows;

    // 3. Ensure enough table slots in memory block
    if (batch.table_batches.size() > block->tables.size()) {
        release_block(block);
        throw std::runtime_error("Not enough table slots in memory block");
    }

    // 4. Process each table
    block->used_tables = batch.table_batches.size();
    for (size_t tbl_idx = 0; tbl_idx < batch.table_batches.size(); ++tbl_idx) {
        const auto& [table_name, rows] = batch.table_batches[tbl_idx];
        auto& table_block = block->tables[tbl_idx];

        // 5. Set table metadata
        table_block.table_name = table_name.c_str();

        // 6. Ensure enough row space
        if (rows.size() > table_block.max_rows) {
            release_block(block);
            throw std::runtime_error("Not enough row capacity in table block");
        }

        // 7. Fill data
        table_block.add_rows(rows);
    }

    return block;
}