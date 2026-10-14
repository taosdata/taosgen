#pragma once

/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "MemoryPool.hpp"


class PooledMultiBatch {
public:
    // Memory block reference
    MemoryPool::MemoryBlock* block;
    
    // Metadata (directly references data in the memory block)
    int64_t start_time() const noexcept { 
        return block ? block->start_time : std::numeric_limits<int64_t>::max(); 
    }
    
    int64_t end_time() const noexcept { 
        return block ? block->end_time : std::numeric_limits<int64_t>::min(); 
    }
    
    size_t total_rows() const noexcept { 
        return block ? block->total_rows : 0; 
    }
    
    // Table batch information (dynamically calculated, does not store actual data)
    class TableBatchProxy {
    public:
        TableBatchProxy(MemoryPool::TableBlock* tbl) : table(tbl) {}
        
        const std::string& table_name() const { 
            return table->table_name; 
        }
        
        size_t row_count() const { 
            return table->used_rows; 
        }
        
        // Row data accessor (avoids actually constructing RowData objects)
        class RowProxy {
        public:
            RowProxy(MemoryPool::TableBlock* tbl, size_t idx) 
                : table(tbl), row_index(idx) {}
            
            int64_t timestamp() const { 
                return table->timestamps[row_index]; 
            }
            
            // Column data access
            template <typename T>
            T get_column(size_t col_idx) const {
                auto& col = table->columns[col_idx];
                
                if (col.is_nulls[row_index]) {
                    throw std::runtime_error("Column is NULL");
                }
                
                if (col.is_fixed) {
                    return *reinterpret_cast<T*>(
                        static_cast<char*>(col.fixed_data) + 
                        row_index * col.element_size
                    );
                } else {
                    throw std::runtime_error("Var-length column access not supported");
                }
            }
            
            // Get variable-length column data (avoid copying)
            std::string_view get_varchar(size_t col_idx) const {
                auto& col = table->columns[col_idx];
                if (col.is_fixed || col.is_nulls[row_index]) {
                    return {};
                }
                
                return std::string_view(
                    col.var_data + col.var_offsets[row_index],
                    col.lengths[row_index]
                );
            }
            
        private:
            MemoryPool::TableBlock* table;
            size_t row_index;
        };
        
        RowProxy operator[](size_t row_index) {
            return RowProxy(table, row_index);
        }
        
    private:
        MemoryPool::TableBlock* table;
    };
    
    // Table batch accessor
    TableBatchProxy operator[](size_t table_index) {
        return TableBatchProxy(&block->tables[table_index]);
    }
    
    size_t table_count() const {
        return block ? block->used_tables : 0;
    }
    
    // Constructor/Destructor
    explicit PooledMultiBatch(MemoryPool::MemoryBlock* blk) 
        : block(blk) {}
    
    ~PooledMultiBatch() = default;
    
    // Move semantics
    PooledMultiBatch(PooledMultiBatch&& other) noexcept 
        : block(other.block) {
        other.block = nullptr;
    }
    
    PooledMultiBatch& operator=(PooledMultiBatch&& other) noexcept {
        if (this != &other) {
            block = other.block;
            other.block = nullptr;
        }
        return *this;
    }
    
    // Disable copy
    PooledMultiBatch(const PooledMultiBatch&) = delete;
    PooledMultiBatch& operator=(const PooledMultiBatch&) = delete;
};