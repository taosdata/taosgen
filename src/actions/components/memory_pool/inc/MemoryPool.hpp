#pragma once

#include <vector>
#include <atomic>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include "taos.h"
#include "taosws.h"
#include "ColumnConfigInstance.hpp"
#include "concurrentqueue.hpp"
#include "blockingconcurrentqueue.hpp"
#include "ColumnConverter.hpp"
#include "TableData.hpp"


class MemoryPool {
public:
    struct TableBlock {
        struct Column {
            bool is_fixed;
            size_t element_size;  // Fixed column: element size
            size_t max_length;    // Variable column: max length
            
            // Fixed column data pointer
            void* fixed_data = nullptr;
            
            // Variable column data
            char* var_data = nullptr;      // Continuous storage area
            int32_t* lengths = nullptr;    // Length per row
            size_t* var_offsets = nullptr; // Offset per row in variable data area
            size_t current_offset = 0;     // Current write offset
            
            // NULL marker
            char* is_nulls = nullptr;
        };
        
        const char* table_name;
        int64_t* timestamps = nullptr;  // Timestamp column continuous storage
        size_t max_rows = 0;
        size_t used_rows = 0;           // Actual used row count
        std::vector<Column> columns;
        const std::vector<ColumnConverter::ColumnHandler>* col_handlers_ptr = nullptr;


        // Add a row to the table (efficient implementation)
        void add_row(const RowData& row) {
            // Write timestamp
            timestamps[used_rows] = row.timestamp;
    
            // Process by column
            for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
                auto& col_block = columns[col_idx];
                const auto& col_value = row.columns[col_idx];
                const auto& handler = (*col_handlers_ptr)[col_idx];
                
                // Handle NULL value
                // if (std::holds_alternative<std::monostate>(col_value)) {
                //     col_block.is_nulls[used_rows] = 1;
                //     continue;
                // }
                
                col_block.is_nulls[used_rows] = 0;
                
                if (col_block.is_fixed) {
                    // Fixed-length column
                    void* dest = static_cast<char*>(col_block.fixed_data) 
                            + used_rows * col_block.element_size;
                    
                    handler.fixed_handler(col_value, dest, col_block.element_size);
                } else {
                    // Variable-length column
                    char* dest = col_block.var_data + col_block.current_offset;
                    
                    size_t data_len = handler.var_handler(col_value, dest, col_block.max_length);
                    
                    // Update metadata
                    col_block.lengths[used_rows] = static_cast<int32_t>(data_len);
                    col_block.var_offsets[used_rows] = col_block.current_offset;
                    col_block.current_offset += data_len;
                }
            }

            used_rows += 1;
        }

        // Batch add rows (more efficient version)
        void add_rows(const std::vector<RowData>& rows) {
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
                        
                        handler.fixed_handler(col_value, dest, col_block.element_size);
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
                        
                        size_t data_len = handler.var_handler(
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
    };

    struct MemoryBlock {
        std::vector<TableBlock> tables;
        int64_t start_time = std::numeric_limits<int64_t>::max();
        int64_t end_time = std::numeric_limits<int64_t>::min();
        size_t total_rows = 0;
        size_t used_tables = 0;         // Actual used table count
        size_t col_count = 0;
        bool in_use = false;
        MemoryPool* owning_pool = nullptr;

        void* data_chunk = nullptr;  // Continuous memory block for all data
        size_t data_chunk_size = 0;  // Memory block size

        TAOS_STMT2_BINDV bindv_{};
        std::vector<const char*> tbnames_;          // Table name pointer array
        std::vector<TAOS_STMT2_BIND*> bind_ptrs_;   // Bind pointer array
        std::vector<std::vector<TAOS_STMT2_BIND>> bind_lists_; // Bind data storage

        void release() {
            if (owning_pool) {
                owning_pool->release_block(this);
            }
        }

        // Release large memory block
        void release_data_chunk() {
            if (data_chunk) {
                std::free(data_chunk);
                data_chunk = nullptr;
                data_chunk_size = 0;
            }
        }

        // Initialize bindv structure
        void init_bindv() {
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

        void build_bindv() {
            bindv_.count = used_tables;
            
            // Update table names and row counts
            for (size_t i = 0; i < used_tables; ++i) {
                auto& table = tables[i];
                tbnames_[i] = table.table_name;
                
                // Update timestamp row count
                bind_lists_[i][0].num = table.used_rows;
                
                // Update data column row count
                for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
                    bind_lists_[i][1 + col_idx].num = table.used_rows;
                }
            }
        }

        // Reset method
        void reset() {
            total_rows = 0;
            start_time = std::numeric_limits<int64_t>::max();
            end_time = std::numeric_limits<int64_t>::min();
            used_tables = 0;
            
            for (auto& table : tables) {
                table.used_rows = 0;
                for (auto& col : table.columns) {
                    col.current_offset = 0;
                    // No need to clear data, will be overwritten later
                    // if (col.is_nulls) { // Is this reset necessary?
                    //     memset(col.is_nulls, 0, table.max_rows * sizeof(char));
                    // }
                }
            }
        
            // Reset bindv count
            bindv_.count = 0;
        }
    };

    MemoryPool(size_t block_count, 
               size_t max_tables_per_block,
               size_t max_rows_per_table,
               const ColumnConfigInstanceVector& col_instances);
               
    ~MemoryPool();
    
    // Get a free memory block (thread-safe)
    MemoryBlock* acquire_block();
    
    // Return a memory block (thread-safe)
    void release_block(MemoryBlock* block);

    // Write MultiBatch data into MemoryBlock
    MemoryBlock* convert_to_memory_block(MultiBatch&& batch);

    const std::vector<ColumnConverter::ColumnHandler>& col_handlers() const {
        return col_handlers_;
    }

private:
    const ColumnConfigInstanceVector& col_instances_;
    std::vector<ColumnConverter::ColumnHandler> col_handlers_;
    std::vector<MemoryBlock> blocks_;
    moodycamel::BlockingConcurrentQueue<MemoryBlock*> free_queue_;
};