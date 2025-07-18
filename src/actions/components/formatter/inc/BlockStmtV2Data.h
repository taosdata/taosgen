#pragma once

#include "IStmtData.h"
#include "MemoryPool.h"


class BlockStmtV2Data : public IStmtData {
public:
    BlockStmtV2Data(MemoryPool::MemoryBlock* block, 
                   const ColumnConfigInstanceVector& col_instances)
        : block_(block), col_instances_(col_instances)
    {
        if (!block_ || block_->used_tables == 0) {
            bindv_.count = 0;
            return;
        }
        
        // Initialize bind structure
        bindv_.count = block_->used_tables;
        tbnames_.reserve(bindv_.count);
        bind_ptrs_.reserve(bindv_.count);
        
        for (size_t i = 0; i < block_->used_tables; ++i) {
            auto& table = block_->tables[i];
            if (table.used_rows == 0) continue; // Skip empty tables
            
            tbnames_.push_back(table.table_name);
            
            std::vector<TAOS_STMT2_BIND> binds;
            binds.reserve(col_instances_.size() + 1);  // +1 for timestamp
            
            // Timestamp column binding
            binds.push_back(TAOS_STMT2_BIND{
                TSDB_DATA_TYPE_TIMESTAMP,
                table.timestamps,
                nullptr,  // Timestamp does not need length array
                nullptr,  // Timestamp does not need NULL marker
                static_cast<int>(table.used_rows)
            });
            
            // Data column binding
            for (size_t col_idx = 0; col_idx < col_instances_.size(); ++col_idx) {
                auto& col = table.columns[col_idx];
                auto& config = col_instances_[col_idx].config();

                TAOS_STMT2_BIND bind = {
                    config.get_taos_type(),
                    col.is_fixed ? col.fixed_data : col.var_data,
                    col.is_fixed ? nullptr : col.lengths,
                    col.is_nulls,
                    static_cast<int>(table.used_rows)
                };

                binds.push_back(bind);
            }
            bind_lists_.push_back(std::move(binds));
            bind_ptrs_.push_back(bind_lists_.back().data());
        }
        
        // Set BINDV structure
        if (!tbnames_.empty()) {
            bindv_.tbnames = const_cast<char**>(tbnames_.data());
            bindv_.tags = nullptr;
            bindv_.bind_cols = bind_ptrs_.data();
        } else {
            bindv_.count = 0;
        }
    }
    
    size_t row_count() const noexcept override {
        return block_ ? block_->total_rows : 0;
    }
    
    size_t column_count() const noexcept override {
        return col_instances_.size();
    }
    
    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept override {
        return &bindv_;
    }
    
    MemoryPool::MemoryBlock* get_block() const noexcept {
        return block_;
    }
    
private:
    MemoryPool::MemoryBlock* block_;
    const ColumnConfigInstanceVector& col_instances_;
    
    // Temporary storage for bind structure
    TAOS_STMT2_BINDV bindv_{};
    std::vector<const char*> tbnames_;
    std::vector<std::vector<TAOS_STMT2_BIND>> bind_lists_;
    std::vector<TAOS_STMT2_BIND*> bind_ptrs_;
};