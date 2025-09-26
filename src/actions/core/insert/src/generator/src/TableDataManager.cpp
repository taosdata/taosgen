#include "TableDataManager.hpp"
#include <algorithm>
#include <limits>
#include <iostream>


TableDataManager::TableDataManager(MemoryPool& pool, const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances)
    : pool_(pool), config_(config), col_instances_(col_instances) {

    // Set interlace rows
    if (config_.schema.generation.interlace_mode.enabled) {
        interlace_rows_ = config_.schema.generation.interlace_mode.rows;
    } else {
        interlace_rows_ = std::numeric_limits<int64_t>::max();
    }

    // Initialize flow control
    if (config_.schema.generation.flow_control.enabled) {
        rate_limiter_ = std::make_unique<RateLimiter>(
            config_.schema.generation.flow_control.rate_limit
        );
    }
}

bool TableDataManager::init(const std::vector<std::string>& table_names) {
    if (table_names.empty()) {
        std::cerr << "TableDataManager initialized with empty table list" << std::endl;
        return false;
    }

    active_table_count_ = table_names.size();
    table_states_.clear();
    table_states_.reserve(table_names.size());

    try {
        // Create RowDataGenerator for each table
        for (const auto& table_name : table_names) {
            TableState state;
            try {
                state.table_name = table_name;
                state.generator = std::make_unique<RowDataGenerator>(
                    table_name,
                    config_,
                    col_instances_
                );
                state.rows_generated = 0;
                state.interlace_counter = 0;
                state.completed = false;

                table_states_.push_back(std::move(state));
            } catch (const std::exception& e) {
                // std::cerr << "Failed to create RowDataGenerator for table: " << table_name
                //          << " - " << e.what() << std::endl;
                // return false;
                // state.completed = true;

                throw std::runtime_error(std::string("Failed to create RowDataGenerator for table: ") + table_name + " - " + e.what());
            }
        }

        current_table_index_ = 0;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize TableDataManager: " << e.what() << std::endl;
        return false;
    }
}

void TableDataManager::acquire_tokens(int64_t tokens) {
    rate_limiter_->acquire(tokens);
}

size_t TableDataManager::get_total_rows_generated() const {
    return total_rows_generated_.load(std::memory_order_relaxed);
}

const ColumnConfigInstanceVector& TableDataManager::get_column_instances() const {
    return col_instances_;
}

std::optional<MemoryPool::MemoryBlock*> TableDataManager::next_multi_batch() {
    // if (!has_more()) {
    //     return std::nullopt;
    // }

    // Get maximum rows per request from config
    size_t max_rows = config_.schema.generation.rows_per_batch;
    if (max_rows == 0) {
        max_rows = std::numeric_limits<size_t>::max();
    }

    MemoryPool::MemoryBlock* batch = collect_batch_data(max_rows);
    if (batch == nullptr) {
        return std::nullopt;
    }

    total_rows_generated_.fetch_add(batch->total_rows, std::memory_order_relaxed);
    return batch;
}

MemoryPool::MemoryBlock* TableDataManager::collect_batch_data(size_t max_rows) {
    // Get memory block from memory pool
    MemoryPool::MemoryBlock* block = pool_.acquire_block(sequence_num_++);
    // if (!block) {
    //     return nullptr;  // No available memory block
    // }

    // Initialize memory block state
    // block->reset();
    int64_t start_time = std::numeric_limits<int64_t>::max();
    int64_t end_time = std::numeric_limits<int64_t>::min();
    size_t total_rows = 0;
    size_t table_loops = 0;
    const size_t max_loops = table_states_.size();

    while (total_rows < max_rows &&
           has_more() &&
           table_loops < max_loops &&
           block->used_tables < block->tables.size())
    {
        TableState* table_state = get_next_active_table();
        if (!table_state) break;

        // Get current table slot
        auto& table_block = block->tables[block->used_tables];
        table_block.table_name = table_state->table_name.c_str();

        // Calculate number of rows that can be generated
        size_t remaining = max_rows - total_rows;
        size_t rows_to_generate = std::min(
            calculate_rows_to_generate(*table_state),
            std::min(remaining, table_block.max_rows)
        );

        // Generate data directly in the memory block
        for (size_t i = 0; i < rows_to_generate; ++i) {
            auto row_opt = table_state->generator->next_row(table_block);
            if (row_opt > 0) {
                // Update statistics
                const int64_t ts = table_block.timestamps[table_block.used_rows - 1];
                start_time = std::min(start_time, ts);
                end_time = std::max(end_time, ts);

                total_rows++;
                table_state->rows_generated++;
                table_state->interlace_counter++;
            } else if (row_opt == 0) {
                if (!table_state->completed) {
                    table_state->completed = true;
                    if (active_table_count_ > 0) --active_table_count_;
                }
                break;
            } else {
                --i;
                continue;
            }

            // Flow control processing
            if (rate_limiter_) {
                acquire_tokens(1);
            }
        }

        // If the table has generated data, increase the used table count
        if (table_block.used_rows > 0) {
            block->used_tables++;
        }

        // Update table state
        if (!table_state->completed &&
            table_state->rows_generated >= config_.schema.generation.rows_per_table) {
            table_state->completed = true;
            if (active_table_count_ > 0) --active_table_count_;
        }

        if (table_state->completed ||
            table_state->interlace_counter >= interlace_rows_) {
            advance_to_next_table();
        }

        table_loops++;
    }

    if (total_rows == 0) {
        // No data generated, release the block
        block->release();
        return nullptr;
    }

    // Update memory block metadata
    block->start_time = start_time;
    block->end_time = end_time;
    block->total_rows = total_rows;

    return block;
}

bool TableDataManager::has_more() const {
    return active_table_count_ > 0;
}

std::string TableDataManager::current_table() const {
    if (table_states_.empty()) return "";
    return table_states_[current_table_index_].table_name;
}

const std::vector<TableDataManager::TableState>& TableDataManager::table_states() const {
    return table_states_;
}

TableDataManager::TableState* TableDataManager::get_next_active_table() {
    if (table_states_.empty()) return nullptr;

    // Loop through tables to find one with available data
    size_t start_index = current_table_index_;

    do {
        TableState& state = table_states_[current_table_index_];

        // Check if table still has data and is not completed
        if (!state.completed &&
            state.rows_generated < config_.schema.generation.rows_per_table &&
            state.generator->has_more())
        {
            return &state;
        }

        // Move to next table
        current_table_index_ = (current_table_index_ + 1) % table_states_.size();
    } while (current_table_index_ != start_index);

    return nullptr;
}

size_t TableDataManager::calculate_rows_to_generate(TableState& state) const {
    // Calculate remaining row limit
    int64_t remaining_rows = std::min(
        config_.schema.generation.rows_per_table - state.rows_generated,
        state.generator->has_more() ? std::numeric_limits<int64_t>::max() : 0
    );

    // Calculate number of rows to generate this time
    int64_t rows_to_generate = std::min(
        interlace_rows_ - state.interlace_counter,
        remaining_rows
    );

    // Consider flow control
    // if (config_.schema.generation.flow_control.rate_limit > 0) {
    //     rows_to_generate = std::min(
    //         rows_to_generate,
    //         config_.schema.generation.flow_control.rate_limit / static_cast<int>(table_order_.size())
    //     );
    // }

    return std::max(static_cast<size_t>(1), static_cast<size_t>(rows_to_generate));
}

void TableDataManager::advance_to_next_table() {
    if (table_states_.empty()) return;

    // Reset interlace counter for current table
    table_states_[current_table_index_].interlace_counter = 0;

    // Move to next table
    current_table_index_ = (current_table_index_ + 1) % table_states_.size();
}