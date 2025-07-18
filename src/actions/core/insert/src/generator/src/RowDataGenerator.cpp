#include "RowDataGenerator.h"
#include <stdexcept>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include "ColumnGeneratorFactory.h"
#include "TimestampGenerator.h"
#include "StringUtils.h"


RowDataGenerator::RowDataGenerator(const std::string& table_name, 
                                  const ColumnsConfig& columns_config,
                                  const ColumnConfigInstanceVector& instances,
                                  const InsertDataConfig::Control& control,
                                  const std::string& target_precision)
    : table_name_(table_name), 
      columns_config_(columns_config),
      instances_(instances),
      control_(control),
      target_precision_(target_precision) {

    init_raw_source();

    if (control_.data_generation.data_cache.enabled) {
        init_cache();
    }

    if (control_.data_quality.data_disorder.enabled) {
        init_disorder();
    }
}

void RowDataGenerator::init_cache() {
    // Pre-generate data to fill cache
    cache_.clear();
    cache_.reserve(control_.data_generation.data_cache.cache_size);
    while (cache_.size() < control_.data_generation.data_cache.cache_size && has_more()) {
        if (auto row = fetch_raw_row()) {
            cache_.push_back(*row);
        } else {
            break;
        }
    }
}

void RowDataGenerator::init_disorder() {
    // Initialize disorder intervals
    disorder_intervals_.clear();
    for (const auto& interval : control_.data_quality.data_disorder.intervals) {
        DisorderInterval disorder_interval;
        disorder_interval.start_time = TimestampUtils::parse_timestamp(interval.time_start, target_precision_);
        disorder_interval.end_time = TimestampUtils::parse_timestamp(interval.time_end, target_precision_);
        disorder_interval.ratio = interval.ratio;
        disorder_interval.latency_range = interval.latency_range;
        disorder_intervals_.push_back(disorder_interval);
    }
}

void RowDataGenerator::init_raw_source() {
    if (columns_config_.source_type == "generator") {
        init_generator();
        total_rows_ = control_.data_generation.per_table_rows;
    } else if (columns_config_.source_type == "csv") {
        init_csv_reader();
        total_rows_ = csv_rows_.size();
    } else {
        throw std::invalid_argument("Unsupported source_type: " + columns_config_.source_type);
    }
    
    // Initialize timestamp generator
    if (columns_config_.source_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            columns_config_.generator.timestamp_strategy.timestamp_config
        );
    } else if (columns_config_.source_type == "csv" && columns_config_.csv.timestamp_strategy.strategy_type == "generator") {
        timestamp_generator_ = std::make_unique<TimestampGenerator>(
            std::get<TimestampGeneratorConfig>(columns_config_.csv.timestamp_strategy.timestamp_config)
        );
    }
}

void RowDataGenerator::init_generator() {
    use_generator_ = true;
    
    // Create row generator  
    row_generator_ = std::make_unique<RowGenerator>(instances_);
}

void RowDataGenerator::init_csv_reader() {
    use_generator_ = false;

    csv_precision_ = columns_config_.csv.timestamp_strategy.get_precision();

    // Create ColumnsCSV reader
    columns_csv_ = std::make_unique<ColumnsCSV>(columns_config_.csv, instances_);

    // TODO: ColumnsCSV needs to support table name index interface
    // Get all table data
    std::vector<TableData> all_tables = columns_csv_->generate();
    
    // Find current table data
    bool found = false;
    for (const auto& table_data : all_tables) {
        if (table_data.table_name == table_name_) {
            found = true;

            for (size_t i = 0; i < table_data.rows.size(); i++) {
                RowData row;
                // row.table_name = table_name_;
                row.timestamp = TimestampUtils::convert_timestamp_precision(table_data.timestamps[i], csv_precision_, target_precision_);
                row.columns = table_data.rows[i];
                csv_rows_.push_back(row);
            }
            break;
        }
    }
    
    if (!found) {
        throw std::runtime_error("Table '" + table_name_ + "' not found in CSV file");
    }
}


std::optional<RowData> RowDataGenerator::next_row() {
    if (generated_rows_ >= total_rows_) {
        return std::nullopt;
    }

    // Process delay queue
    process_delay_queue();
    
    // Prefer to get data from cache
    if (!cache_.empty()) {
        auto row = cache_.back();
        cache_.pop_back();
        generated_rows_++;
        return row;
    }
    
    // Get data from raw source
    auto row_opt = fetch_raw_row();
    if (!row_opt) {
        return std::nullopt;
    }
    
    // Update current timeline
    current_timestamp_ = row_opt->timestamp;
    
    // Apply disorder strategy
    auto delayed = apply_disorder(*row_opt);
    if (!delayed) {
        generated_rows_++;
    }
    
    return row_opt;
}

int RowDataGenerator::next_row(MemoryPool::TableBlock& table_block) {
    if (generated_rows_ >= total_rows_) {
        return 0;
    }

    // Process delay queue
    process_delay_queue();
    
    // Prefer to get data from cache
    if (!cache_.empty()) {
        auto row = cache_.back();
        cache_.pop_back();

        // Add row to memory pool
        table_block.add_row(row);
        generated_rows_++;
        return 1;
    }
    
    // Get data from raw source
    auto row_opt = fetch_raw_row();
    if (!row_opt) {
        return 0;
    }
    
    // Update timeline
    current_timestamp_ = row_opt->timestamp;
    
    // Apply disorder strategy
    bool delayed = apply_disorder(*row_opt);
    if (delayed) {
        return -1;
    }

    // Write directly to memory pool
    generated_rows_++;
    table_block.add_row(*row_opt);

    return 1;
}

bool RowDataGenerator::apply_disorder(RowData& row) {
    if (!control_.data_quality.data_disorder.enabled) {
        return false;
    }
    
    // Check if data timestamp is in disorder interval
    for (const auto& interval : disorder_intervals_) {
        if (row.timestamp >= interval.start_time && row.timestamp < interval.end_time) {
            // Decide whether to delay by probability
            if (static_cast<double>(rand()) / RAND_MAX < interval.ratio) {
                int latency = rand() % interval.latency_range;
                int64_t deliver_time = row.timestamp + latency;
                
                // Put into delay queue
                delay_queue_.push(DelayedRow{deliver_time, row});
                row.timestamp = -1;
                return true;
            }
        }
    }
    return false;
}

void RowDataGenerator::process_delay_queue() {
    while (!delay_queue_.empty() && delay_queue_.top().deliver_timestamp <= current_timestamp_) {
        cache_.push_back(delay_queue_.top().row);
        delay_queue_.pop();
    }
}

std::optional<RowData> RowDataGenerator::fetch_raw_row() {
    RowData row_data;
    
    try {
        if (use_generator_) {
            row_data = generate_from_generator();
        } else {
            if (auto csv_row = generate_from_csv()) {
                row_data = std::move(*csv_row);
            } else {
                total_rows_ = generated_rows_;
                return std::nullopt;
            }
        }

        return row_data;
    } catch (const std::exception& e) {
        std::cerr << "Error generating row: " << e.what() << std::endl;
        return std::nullopt;
    }
}

bool RowDataGenerator::has_more() const {
    return generated_rows_ < total_rows_;
}

void RowDataGenerator::reset() {
    generated_rows_ = 0;
    csv_row_index_ = 0;
    
    if (timestamp_generator_) {
        timestamp_generator_->reset();
    }
}

RowData RowDataGenerator::generate_from_generator() {
    RowData row_data;
    // row_data.table_name = table_name_;
    
    // Generate timestamp
    row_data.timestamp = TimestampUtils::convert_timestamp_precision(timestamp_generator_->generate(),
        timestamp_generator_->timestamp_precision(), target_precision_);
    
    // Generate column data
    row_data.columns = row_generator_->generate();
    
    return row_data;
}

std::optional<RowData> RowDataGenerator::generate_from_csv() {
    return csv_rows_[csv_row_index_++];;
}