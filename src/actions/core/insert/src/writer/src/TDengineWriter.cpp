#include "TDengineWriter.hpp"
#include <iostream>
#include <stdexcept>
#include <thread>
#include "TimeRecorder.hpp"

TDengineWriter::TDengineWriter(const InsertDataConfig& config)
        : config_(config),
          timestamp_precision_(config.target.timestamp_precision),
          time_strategy_(config.control.time_interval, config.target.timestamp_precision),
          start_write_time_(std::chrono::steady_clock::now()),
          end_write_time_(std::chrono::steady_clock::now()) {
    
    // Validate timestamp precision
    if (timestamp_precision_.empty()) {
        timestamp_precision_ = "ms";
    } else if (timestamp_precision_ != "ms" && 
               timestamp_precision_ != "us" && 
               timestamp_precision_ != "ns") {
        throw std::invalid_argument("Invalid timestamp precision: " + timestamp_precision_);
    }
}

TDengineWriter::~TDengineWriter() {
    close();
}

bool TDengineWriter::connect() {
    if (connector_) {
        // Already connected
        return true;
    }
    
    try {
        // Create connector
        connector_ = DatabaseConnector::create(
            config_.control.data_channel, 
            config_.target.tdengine.connection_info
        );
        
        // Establish connection
        return connector_->connect();
    } catch (const std::exception& e) {
        std::cerr << "TDengineWriter connection failed: " << e.what() << std::endl;
        connector_.reset();
        return false;
    }
}

bool TDengineWriter::select_db(const std::string& db_name) {
    return connector_->select_db(db_name);
}

bool TDengineWriter::prepare(const std::string& sql) {
    return connector_->prepare(sql);
}

void TDengineWriter::write(const BaseInsertData& data) {
    if (!connector_) {
        throw std::runtime_error("TDengineWriter is not connected");
    }
    
    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool write_success = false;
    // TimeRecorder timer;
    switch(data.type) {
        case BaseInsertData::DataType::SQL:
            write_success = handle_insert(static_cast<const SqlInsertData&>(data));
            break;
        case BaseInsertData::DataType::STMT_V2:
            write_success = handle_insert(static_cast<const StmtV2InsertData&>(data));
            break;
        default:
            throw std::runtime_error("Unsupported data type");
    }
    // write_metrics_.add_sample(timer.elapsed());

    // Update state
    if (write_success) {
        end_write_time_ = std::chrono::steady_clock::now();
        last_start_time_ = data.start_time;
        last_end_time_ = data.end_time;
        current_retry_count_ = 0;
        first_write_ = false;
    }
}

void TDengineWriter::close() noexcept {
    if (connector_) {
        try {
            connector_->close();
        } catch (const std::exception& e) {
            // Ignore exception on close
        }
        connector_.reset();
    }
}

template<typename T>
bool TDengineWriter::handle_insert(const T& data) {
    const size_t MAX_RETRY = 1;
    while (current_retry_count_ < MAX_RETRY) {
        try {
            if (is_literal_strategy()) {
                int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count();
                int64_t start_time = static_cast<const BaseInsertData&>(data).start_time;
                int64_t elapsed_ms = now_ms - start_time;
                // std::cout << "now_ms=" << now_ms << ", start_time=" << start_time << ", elapsed_ms=" << elapsed_ms << std::endl;
                play_metrics_.add_sample(elapsed_ms);
            }
            TimeRecorder timer;
            bool success = connector_->execute(data);
            write_metrics_.add_sample(timer.elapsed());
            if (success) {
                current_retry_count_ = 0;
                return true;
            }

            if (config_.control.insert_control.failure_handling.on_failure == "exit") {
                throw std::runtime_error(get_format_description() + " insert failed: " + 
                    std::string(typeid(data).name()));
            }
            return false;
            current_retry_count_++;
            // std::this_thread::sleep_for(std::chrono::milliseconds(100 * current_retry_count_));
        } catch (const std::exception& e) {
            if (current_retry_count_ >= MAX_RETRY - 1) {
                throw std::runtime_error(get_format_description() + " insert failed after " + 
                    std::to_string(MAX_RETRY) + " retries: " + e.what());
            }
            current_retry_count_++;
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * current_retry_count_));
        }
    }
    return false;
}

std::string TDengineWriter::get_format_description() const {
    return config_.control.data_format.format_type;
}

void TDengineWriter::apply_time_interval_strategy(int64_t current_start, int64_t current_end) {
    time_strategy_.apply_wait_strategy(
        current_start, current_end,
        last_start_time_, last_end_time_,
        first_write_
    );
    if (first_write_ == true) {
        start_write_time_ = time_strategy_.last_write_time();
    }
}