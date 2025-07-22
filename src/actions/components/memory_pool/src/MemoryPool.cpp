#include "MemoryPool.h"
#include <iostream>
#include <cstring>
#include <new>
#include <limits>
#include <stdexcept>

MemoryPool::MemoryPool(size_t block_count, 
                       size_t max_tables_per_block,
                       size_t max_rows_per_table,
                       const ColumnConfigInstanceVector& col_instances)
    : col_instances_(col_instances),
      col_handlers_(ColumnConverter::create_handlers_for_columns(col_instances)),
      blocks_(block_count),
      free_queue_(block_count)
{
    size_t max_rows_per_block = max_tables_per_block * max_rows_per_table;
    const size_t col_count = col_instances.size();
    
    // 时间戳部分总大小
    const size_t timestamps_size = max_rows_per_block * sizeof(int64_t);
    
    // 计算列总大小
    size_t common_meta_size = 0;
    size_t fixed_data_size = 0;
    size_t var_meta_size = 0;
    size_t var_data_size = 0;

    for (const auto& col_instance : col_instances) {
        // is_nulls
        common_meta_size += max_rows_per_block * (sizeof(char));

        if (col_instance.config().is_var_length()) {
            // lengths + offsets
            var_meta_size += max_rows_per_block * (sizeof(int32_t) + sizeof(size_t));
            var_data_size += max_rows_per_block * col_instance.config().len.value();

        } else {
            fixed_data_size += max_rows_per_block * col_instance.config().get_fixed_type_size();
        }
    }

    // 总内存大小 (64字节对齐)
    const size_t total_block_size = timestamps_size + common_meta_size + fixed_data_size + var_meta_size + var_data_size;
    const size_t aligned_size = ((total_block_size + 63) / 64) * 64;

    for (auto& block : blocks_) {
        block.owning_pool = this;

        // 分配单个大块内存 (64字节对齐)
        block.data_chunk = std::aligned_alloc(64, aligned_size);
        block.data_chunk_size = aligned_size;
        if (!block.data_chunk) {
            throw std::bad_alloc();
        }

        // 初始化内存为0
        std::memset(block.data_chunk, 0, aligned_size);
        
        char* current_ptr = static_cast<char*>(block.data_chunk);
        
        // 1. 分配时间戳内存
        int64_t* timestamps_base = reinterpret_cast<int64_t*>(current_ptr);
        current_ptr += timestamps_size;
        
        // 2. 分配定长列内存
        void* fixed_data_base = current_ptr;
        current_ptr += fixed_data_size;
        
        // 3. 分配变长列元数据内存
        void* var_meta_base = current_ptr;
        current_ptr += var_meta_size;
        
        // 4. 分配变长列数据内存
        char* var_data_base = current_ptr;
        current_ptr += var_data_size;
        
        // 5. 分配公用元数据内存
        char* common_meta_base = current_ptr;
        current_ptr += common_meta_size;
        
        // 初始化表结构
        block.tables.resize(max_tables_per_block);

        // 初始化表元数据
        for (size_t i = 0; i < max_tables_per_block; ++i) {
            auto& table = block.tables[i];
            table.max_rows = max_rows_per_table;
            table.col_handlers_ptr = &col_handlers_;
            table.columns.resize(col_instances.size());

            // 设置时间戳指针
            table.timestamps = timestamps_base + i * max_rows_per_table;
        }
        
        // 设置列指针
        char* fixed_col_ptr = static_cast<char*>(fixed_data_base);
        char* var_meta_ptr = static_cast<char*>(var_meta_base);
        char* var_data_ptr = var_data_base;
        char* common_meta_ptr = common_meta_base;

        for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
            const auto& config = col_instances[col_idx].config();
            const size_t is_nulls_size = max_rows_per_table * sizeof(char);

            if (config.is_var_length()) {
                const size_t lengths_size = max_rows_per_table * sizeof(int32_t);
                const size_t offsets_size = max_rows_per_table * sizeof(size_t);
                const size_t col_data_size = max_rows_per_table * config.len.value();
                
                for (size_t table_idx = 0; table_idx < max_tables_per_block; ++table_idx) {
                    auto& col = block.tables[table_idx].columns[col_idx];
                    col.is_fixed = false;
                    col.max_length = config.len.value();
                    col.element_size = 0;
                    
                    // lengths 数组
                    col.lengths = reinterpret_cast<int32_t*>(var_meta_ptr);
                    var_meta_ptr += lengths_size;
                    
                    // offsets 数组
                    col.var_offsets = reinterpret_cast<size_t*>(var_meta_ptr);
                    var_meta_ptr += offsets_size;
                    
                    // is_nulls 数组
                    col.is_nulls = common_meta_ptr;
                    common_meta_ptr += is_nulls_size;
    
                    // var_data 数据区
                    col.var_data = var_data_ptr;
                    var_data_ptr += col_data_size;
                }
            } else {
                const size_t col_data_size = max_rows_per_table * config.get_fixed_type_size();
                
                for (size_t table_idx = 0; table_idx < max_tables_per_block; ++table_idx) {
                    auto& col = block.tables[table_idx].columns[col_idx];
                    col.is_fixed = true;
                    col.element_size = config.get_fixed_type_size();
                    col.max_length = col.element_size;

                    // is_nulls 数组
                    col.is_nulls = common_meta_ptr;
                    common_meta_ptr += is_nulls_size;

                    // fixed_data 数据区
                    col.fixed_data = fixed_col_ptr;
                    fixed_col_ptr += col_data_size;
                }
            }
        }
        
        // 初始化bindv结构
        block.init_bindv();

        // Initial state: memory block is free
        block.reset();
        free_queue_.enqueue(&block);
    }
}

MemoryPool::~MemoryPool() {
    for (auto& block : blocks_) {
        block.owning_pool = nullptr;
        block.release_data_chunk();
    }
}

MemoryPool::MemoryBlock* MemoryPool::acquire_block() {
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
    block->reset();
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