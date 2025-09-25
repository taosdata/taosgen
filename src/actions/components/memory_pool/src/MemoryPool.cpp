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

// TableBase
void MemoryPool::TableBase::fill_row(size_t row_index, const RowData& row) {
    if (row_index >= max_rows) {
        throw std::out_of_range("row_index out of range in cached table block");
    }

    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        auto& col = columns[col_idx];
        const auto& col_value = row.columns[col_idx];
        const auto& handler = (*col_handlers_ptr)[col_idx];

        // Handle NULL value
        // if (std::holds_alternative<std::monostate>(col_value)) {
        //     col_block.is_nulls[used_rows] = 1;
        //     continue;
        // }

        col.is_nulls[row_index] = 0;

        if (col.is_fixed) {
            // Fixed-length column
            void* dest = static_cast<char*>(col.fixed_data) + row_index * col.element_size;
            handler.to_fixed(col_value, dest, col.element_size);
        } else {
            // Variable-length column
            char* dest = col.var_data + col.current_offset;
            size_t data_len = handler.to_var(col_value, dest, col.max_length);

            // Update metadata
            col.lengths[row_index] = static_cast<int32_t>(data_len);
            col.var_offsets[row_index] = col.current_offset;
            col.current_offset += data_len;
        }
    }

    used_rows++;
}

// CachedTableBlock
void MemoryPool::CachedTableBlock::fill_cached_data(size_t row_index, const RowData& row) {
    fill_row(row_index, row);
}

void MemoryPool::CachedTableBlock::fill_cached_data_batch(const std::vector<RowData>& rows, size_t start_row) {
    if (start_row + rows.size() > max_rows) {
        throw std::out_of_range("Not enough space in cached table block");
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        fill_cached_data(start_row + i, rows[i]);
    }
    data_prefilled = true;
}

// TableBlock
void MemoryPool::TableBlock::init_from_cache(const CachedTableBlock& cached_block) {
    max_rows = cached_block.max_rows;
    col_handlers_ptr = cached_block.col_handlers_ptr;
    cached_table_block = &cached_block;

    columns.resize(cached_block.columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i] = cached_block.columns[i];
        columns[i].current_offset = 0;
    }
}

void MemoryPool::TableBlock::add_row_cached(const RowData& row) {
    timestamps[used_rows] = row.timestamp;
    used_rows++;
}

void MemoryPool::TableBlock::add_rows_cached(const std::vector<RowData>& rows) {
    size_t start_index = used_rows;

    // Ensure enough space
    if (start_index + rows.size() > max_rows) {
        throw std::out_of_range("Not enough space in table block");
    }

    // Process timestamps only
    for (size_t i = 0; i < rows.size(); ++i) {
        timestamps[start_index + i] = rows[i].timestamp;
    }

    used_rows += rows.size();
}

void MemoryPool::TableBlock::add_row(const RowData& row) {
    if (cached_table_block) {
        add_row_cached(row);
    } else {
        timestamps[used_rows] = row.timestamp;
        fill_row(used_rows, row);
    }
}

void MemoryPool::TableBlock::add_rows(const std::vector<RowData>& rows) {
    size_t start_index = used_rows;

    // Ensure enough space
    if (start_index + rows.size() > max_rows) {
        throw std::out_of_range("Not enough space in table block");
    }

    // Process all timestamps first
    for (size_t i = 0; i < rows.size(); ++i) {
        timestamps[start_index + i] = rows[i].timestamp;
    }

    // Process by column, improve cache utilization
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        auto& col_block = columns[col_idx];
        const auto& handler = (*col_handlers_ptr)[col_idx];

        if (col_block.is_fixed) {
            // Fixed-length column - batch copy
            for (size_t i = 0; i < rows.size(); ++i) {
                const auto& col_value = rows[i].columns[col_idx];

                // Handle NULL value
                // if (std::holds_alternative<std::monostate>(col_value)) {
                //     col_block.is_nulls[start_index + i] = 1;
                //     continue;
                // }

                col_block.is_nulls[start_index + i] = 0;

                void* dest = static_cast<char*>(col_block.fixed_data)
                        + (start_index + i) * col_block.element_size;

                handler.to_fixed(col_value, dest, col_block.element_size);
            }
        } else {
            // Batch copy data
            for (size_t i = 0; i < rows.size(); ++i) {
                if (col_block.is_nulls[start_index + i]) continue;

                const auto& col_value = rows[i].columns[col_idx];

                // if (std::holds_alternative<std::monostate>(col_value)) {
                //     col_block.is_nulls[start_index + i] = 1;
                //     continue;
                // }

                col_block.is_nulls[start_index + i] = 0;

                char* dest = col_block.var_data + col_block.current_offset;

                size_t data_len = handler.to_var(
                    col_value, dest, col_block.max_length
                );

                // Update metadata
                col_block.lengths[start_index + i] = static_cast<int32_t>(data_len);
                col_block.var_offsets[start_index + i] = col_block.current_offset;
                col_block.current_offset += data_len;
            }
        }
    }

    // Update used row count
    used_rows += rows.size();
}

ColumnType MemoryPool::TableBlock::get_cell(size_t row_index, size_t col_index) const {
    if (row_index >= used_rows)
        throw std::out_of_range("row_index out of range");
    if (col_index >= columns.size())
        throw std::out_of_range("col_index out of range");

    const auto& col = columns[col_index];
    const auto& handler = (*col_handlers_ptr)[col_index];

    if (col.is_nulls[row_index])
        throw std::runtime_error("NULL value not supported");

    if (col.is_fixed) {
        // Fixed-length column handling
        void* data_ptr = static_cast<char*>(col.fixed_data) + row_index * col.element_size;
        return handler.to_column_fixed(data_ptr);
    } else {
        // Variable-length column handling
        char* data_start = col.var_data + col.var_offsets[row_index];
        return handler.to_column_var(data_start, col.lengths[row_index]);
    }
}

std::string MemoryPool::TableBlock::get_cell_as_string(size_t row_index, size_t col_index) const {
    ColumnType cell = get_cell(row_index, col_index);
    const auto& handler = (*col_handlers_ptr)[col_index];
    return handler.to_string(cell);
}

// MemoryBlock
void MemoryPool::MemoryBlock::release() {
    if (owning_pool) {
        owning_pool->release_block(this);
    }
}

void MemoryPool::MemoryBlock::free_data_chunk() {
    if (data_chunk) {
        std::free(data_chunk);
        data_chunk = nullptr;
        data_chunk_size = 0;
    }
}

void MemoryPool::MemoryBlock::init_bindv() {
    size_t max_tables = tables.size();
    const ColumnConfigInstanceVector& col_instances = owning_pool->col_instances_;
    col_count = col_instances.size();

    // Pre-allocate data structures
    tbnames_.resize(max_tables, nullptr);
    bind_ptrs_.resize(max_tables, nullptr);
    bind_lists_.resize(max_tables);

    // Pre-allocate bind list for each table
    for (size_t i = 0; i < max_tables; ++i) {
        bind_lists_[i].resize(1 + col_count);
        bind_ptrs_[i] = bind_lists_[i].data();
        auto& table = tables[i];

        // Initialize timestamp column binding
        bind_lists_[i][0] = {
            TSDB_DATA_TYPE_TIMESTAMP,
            table.timestamps,
            nullptr,
            nullptr,
            static_cast<int>(table.max_rows)
        };

        // Initialize data column binding
        for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
            auto& bind = bind_lists_[i][1 + col_idx];
            const auto& config = col_instances[col_idx].config();
            auto& col = table.columns[col_idx];

            bind.buffer_type = config.get_taos_type();
            bind.num = static_cast<int>(table.max_rows);

            if (config.is_var_length()) {
                bind.buffer = col.var_data;
                bind.length = col.lengths;
                bind.is_null = col.is_nulls;
            } else {
                bind.buffer = col.fixed_data;
                bind.length = nullptr;
                bind.is_null = col.is_nulls;
            }
        }
    }

    // Set pointers
    bindv_.tbnames = const_cast<char**>(tbnames_.data());
    bindv_.bind_cols = bind_ptrs_.data();
}

void MemoryPool::MemoryBlock::build_bindv(bool is_checkpoint_recover) {
    bindv_.count = used_tables;

    // Update table names and row counts
    for (size_t i = 0; i < used_tables; ++i) {
        auto& table = tables[i];
        tbnames_[i] = table.table_name;

        if (is_checkpoint_recover) {
            checkpoint_data_list_.emplace_back(CheckpointData(table.table_name, table.timestamps[table.used_rows - 1], table.used_rows));
        }

        // Update timestamp row count
        bind_lists_[i][0].num = table.used_rows;

        // Update data column row count
        for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
            bind_lists_[i][1 + col_idx].num = table.used_rows;
        }
    }
}

void MemoryPool::MemoryBlock::reset() {
    total_rows = 0;
    start_time = std::numeric_limits<int64_t>::max();
    end_time = std::numeric_limits<int64_t>::min();
    used_tables = 0;

    for (auto& table : tables) {
        table.used_rows = 0;
        for (auto& col : table.columns) {
            col.current_offset = 0;
        }
    }
    bindv_.count = 0;
}

// CacheUnit
MemoryPool::CacheUnit::~CacheUnit() {
    if (data_chunk) {
        std::free(data_chunk);
        data_chunk = nullptr;
        data_chunk_size = 0;
    }
}

MemoryPool::CacheUnit::CacheUnit(CacheUnit&& other) noexcept
    : tables(std::move(other.tables)),
    data_chunk(other.data_chunk),
    data_chunk_size(other.data_chunk_size),
    data_prefilled(other.data_prefilled.load())
{
    other.data_chunk = nullptr;
    other.data_chunk_size = 0;
    other.data_prefilled = false;
}

MemoryPool::CacheUnit& MemoryPool::CacheUnit::operator=(CacheUnit&& other) noexcept {
    if (this != &other) {
        if (data_chunk) {
            std::free(data_chunk);
        }

        tables = std::move(other.tables);
        data_chunk = other.data_chunk;
        data_chunk_size = other.data_chunk_size;
        data_prefilled = other.data_prefilled.load();

        other.data_chunk = nullptr;
        other.data_chunk_size = 0;
        other.data_prefilled = false;
    }
    return *this;
}

// MemoryPool
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

    if (num_cached_blocks_ > 0) {
        init_cache_units();
        init_cached_blocks();
    } else {
        init_normal_blocks();
    }

    for (auto& block : blocks_) {
        free_queue_.enqueue(&block);
    }
}

MemoryPool::~MemoryPool() {
    for (auto& block : blocks_) {
        block.owning_pool = nullptr;
        block.free_data_chunk();
    }
}

void MemoryPool::init_cache_units() {
    cache_units_.resize(num_cached_blocks_);

    for (size_t cache_idx = 0; cache_idx < num_cached_blocks_; ++cache_idx) {
        auto& cache_unit = cache_units_[cache_idx];

        cache_unit.data_chunk = std::aligned_alloc(MEMORY_POOL_ALIGNMENT, total_cache_size_);
        cache_unit.data_chunk_size = total_cache_size_;

        if (!cache_unit.data_chunk) {
            throw std::bad_alloc();
        }

        // Initialize memory to zero
        std::memset(cache_unit.data_chunk, 0, cache_unit.data_chunk_size);
        char* current_ptr = static_cast<char*>(cache_unit.data_chunk);

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
        cache_unit.tables.resize(max_tables_per_block_);

        // Set column pointers
        char* fixed_col_ptr = static_cast<char*>(fixed_data_base);
        char* var_meta_ptr = static_cast<char*>(var_meta_base);
        char* var_data_ptr = var_data_base;
        char* common_meta_ptr = common_meta_base;

        const size_t is_nulls_size = max_rows_per_table_ * sizeof(char);
        const size_t lengths_size = max_rows_per_table_ * sizeof(int32_t);
        const size_t offsets_size = max_rows_per_table_ * sizeof(size_t);

        for (size_t table_idx = 0; table_idx < max_tables_per_block_; ++table_idx) {
            auto& cached_table = cache_unit.tables[table_idx];
            cached_table.max_rows = max_rows_per_table_;
            cached_table.col_handlers_ptr = &col_handlers_;
            cached_table.columns.resize(col_instances_.size());

            for (size_t col_idx = 0; col_idx < col_instances_.size(); ++col_idx) {
                auto& col = cached_table.columns[col_idx];
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

        block.data_chunk = std::aligned_alloc(MEMORY_POOL_ALIGNMENT, timestamps_size_);
        block.data_chunk_size = timestamps_size_;

        if (!block.data_chunk) {
            throw std::bad_alloc();
        }

        // Initialize memory to zero
        std::memset(block.data_chunk, 0, block.data_chunk_size);

        block.cache_index = block_idx % num_cached_blocks_;

        // Initialize table structure
        block.tables.resize(max_tables_per_block_);

        // Initialize table metadata
        int64_t* timestamps_base = reinterpret_cast<int64_t*>(block.data_chunk);
        for (size_t i = 0; i < max_tables_per_block_; ++i) {
            auto& table = block.tables[i];
            table.max_rows = max_rows_per_table_;
            table.col_handlers_ptr = &col_handlers_;

            // Set timestamp pointer
            table.timestamps = timestamps_base + i * max_rows_per_table_;
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

    if (block) {
        if (is_cache_mode()) {
            size_t cache_index = sequence_num % num_cached_blocks_;
            block->cache_index = cache_index;
            auto& cache_unit = cache_units_[cache_index];

            for (size_t table_idx = 0; table_idx < max_tables_per_block_; ++table_idx) {
                auto& table = block->tables[table_idx];
                auto& cached_table = cache_unit.tables[table_idx];

                table.init_from_cache(cached_table);
            }

            block->init_bindv();
        }

        block->in_use = true;
        block->reset();
    }

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

const std::vector<ColumnConverter::ColumnHandler>& MemoryPool::col_handlers() const {
    return col_handlers_;
}

bool MemoryPool::is_cache_mode() const {
    return num_cached_blocks_ > 0;
}

MemoryPool::CacheUnit* MemoryPool::get_cache_unit(size_t index) {
    if (index < cache_units_.size()) {
        return &cache_units_[index];
    }
    return nullptr;
}

size_t MemoryPool::get_cache_units_count() const {
    return cache_units_.size();
}

bool MemoryPool::fill_cache_unit_data(size_t cache_index, size_t table_index, const std::vector<RowData>& data_rows) {
    if (cache_index >= cache_units_.size()) {
        return false;
    }

    auto& cache_unit = cache_units_[cache_index];
    if (table_index >= cache_unit.tables.size()) {
        return false;
    }

    auto& cached_table = cache_unit.tables[table_index];
    cached_table.fill_cached_data_batch(data_rows);
    cache_unit.data_prefilled = true;

    return true;
}

bool MemoryPool::is_cache_unit_prefilled(size_t cache_index) const {
    if (cache_index < cache_units_.size()) {
        return cache_units_[cache_index].data_prefilled.load();
    }
    return false;
}