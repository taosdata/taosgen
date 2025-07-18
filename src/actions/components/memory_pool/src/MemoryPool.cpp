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
    : col_instances_(col_instances), col_handlers_(ColumnConverter::create_handlers_for_columns(col_instances))
{
    blocks_.resize(block_count);
    
    for (auto& block : blocks_) {
        block.tables.reserve(max_tables_per_block);
        
        for (size_t i = 0; i < max_tables_per_block; ++i) {
            TableBlock table;
            table.max_rows = max_rows_per_table;
            table.col_handlers_ptr = &col_handlers_;
            
            // Allocate timestamp column (64-byte aligned)
            table.timestamps = static_cast<int64_t*>(
                std::aligned_alloc(64, max_rows_per_table * sizeof(int64_t)));
            if (!table.timestamps) {
                throw std::bad_alloc();
            }
            
            // Initialize each column
            for (const auto& col_instance : col_instances) {
                TableBlock::Column col;
                const auto& config = col_instance.config();
                col.is_fixed = !config.is_var_length();
                
                if (col.is_fixed) {
                    // Fixed-length column
                    col.element_size = config.get_fixed_type_size();
                    col.max_length = col.element_size;
                    
                    col.fixed_data = std::aligned_alloc(64, 
                        max_rows_per_table * col.element_size);
                    if (!col.fixed_data) {
                        throw std::bad_alloc();
                    }
                } else {
                    // Variable-length column
                    col.max_length = config.len.value();
                    col.element_size = 0; // Not used for variable-length column
                    
                    // Allocate variable-length data area
                    col.var_data = static_cast<char*>(std::aligned_alloc(64,
                        max_rows_per_table * col.max_length));
                    if (!col.var_data) {
                        throw std::bad_alloc();
                    }
                    
                    // Allocate length array
                    col.lengths = static_cast<int32_t*>(std::aligned_alloc(64,
                        max_rows_per_table * sizeof(int32_t)));
                    if (!col.lengths) {
                        throw std::bad_alloc();
                    }
                    
                    // Allocate offset array
                    col.var_offsets = static_cast<size_t*>(std::aligned_alloc(64,
                        max_rows_per_table * sizeof(size_t)));
                    if (!col.var_offsets) {
                        throw std::bad_alloc();
                    }
                }
                
                // Allocate NULL marker array
                col.is_nulls = static_cast<char*>(std::aligned_alloc(64,
                    max_rows_per_table * sizeof(char)));
                if (!col.is_nulls) {
                    throw std::bad_alloc();
                }
                
                // Initialize NULL marker to 0 (not NULL)
                memset(col.is_nulls, 0, max_rows_per_table * sizeof(char));
                
                table.columns.push_back(std::move(col));
            }
            block.tables.push_back(std::move(table));
        }
        
        // Initial state: memory block is free
        block.reset();
        free_queue_.enqueue(&block);
    }
}

MemoryPool::~MemoryPool() {
    for (auto& block : blocks_) {
        for (auto& table : block.tables) {
            // Free timestamp column
            if (table.timestamps) {
                std::free(table.timestamps);
                table.timestamps = nullptr;
            }

            // Free memory for each column
            for (auto& col : table.columns) {
                if (col.fixed_data) {
                    std::free(col.fixed_data);
                    col.fixed_data = nullptr;
                }
                if (col.var_data) {
                    std::free(col.var_data);
                    col.var_data = nullptr;
                }
                if (col.lengths) {
                    std::free(col.lengths);
                    col.lengths = nullptr;
                }
                if (col.var_offsets) {
                    std::free(col.var_offsets);
                    col.var_offsets = nullptr;
                }
                if (col.is_nulls) {
                    std::free(col.is_nulls);
                    col.is_nulls = nullptr;
                }
            }
        }
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