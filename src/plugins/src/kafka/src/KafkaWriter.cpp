#include "KafkaWriter.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

KafkaWriter::KafkaWriter(const InsertDataConfig& config, size_t no)
    : BaseWriter(config) {
    client_ = std::make_unique<KafkaClient>(config.kafka, config.data_format.kafka, no);
}

KafkaWriter::~KafkaWriter() {
    close();
}

bool KafkaWriter::connect(std::optional<ConnectorSource>& conn_source) {
    (void)conn_source;

    if (client_ && client_->is_connected()) {
        return true;
    }

    try {
        return client_->connect();
    } catch (const std::exception& e) {
        LogUtils::error("KafkaWriter connection failed: {}", e.what());
        return false;
    }
}

bool KafkaWriter::prepare(const std::string& context) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("KafkaWriter is not connected");
    }
    return client_->prepare(context);
}

bool KafkaWriter::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("KafkaWriter is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        switch(data.type) {
            case BaseInsertData::DataType::KAFKA:
                success = execute_with_retry([&] {
                    return handle_insert(static_cast<const KafkaInsertData&>(data));
                }, "kafka message insert");
                break;
            default:
                throw std::runtime_error("Unsupported data type for KafkaWriter: " + std::to_string(static_cast<int>(data.type)));
        }
    } catch (const std::exception& e) {
        if (config_.failure_handling.on_failure == "exit") {
            throw;
        }
    }

    update_write_state(data, success);
    return success;
}

template<typename T>
bool KafkaWriter::handle_insert(const T& data) {
    if (time_strategy_.is_literal_strategy()) {
        update_play_metrics(data);
    }

    TimeRecorder timer;
    bool success = client_->execute(data);
    write_metrics_.add_sample(timer.elapsed());

    return success;
}

void KafkaWriter::close() noexcept {
    if (client_) {
        try {
            client_->close();
        } catch (const std::exception& e) {
            LogUtils::error("Exception during KafkaWriter close: {}", e.what());
        }
        client_.reset();
    }
}

void KafkaWriter::set_client(std::unique_ptr<KafkaClient> client) {
    client_ = std::move(client);
}

KafkaClient* KafkaWriter::get_client() {
    return client_.get();
}