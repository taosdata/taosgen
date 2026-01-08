#include "KafkaWriter.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include <stdexcept>

KafkaWriter::KafkaWriter(const InsertDataConfig& config, size_t no)
    : BaseWriter(config) {

    const auto* kc = get_plugin_config<KafkaConfig>(config.extensions, "kafka");
    if (kc == nullptr) {
        throw std::runtime_error("Kafka configuration not found in insert extensions");
    }

    const auto* kf = get_format_opt<KafkaFormatOptions>(config.data_format, "kafka");
    if (kf == nullptr) {
        throw std::runtime_error("Kafka format options not found in DataFormat");
    }

    if (no == 0) {
        LogUtils::info("Inserting data into: {}", kc->get_sink_info());
    }

    client_ = std::make_unique<KafkaClient>(*kc, *kf, no);
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

bool KafkaWriter::write(const BaseInsertData& data) {
    if (!client_ || !client_->is_connected()) {
        throw std::runtime_error("KafkaWriter is not connected");
    }

    // Apply time interval strategy
    apply_time_interval_strategy(data.start_time, data.end_time);

    // Execute write
    bool success = false;
    try {
        if (data.type == KAFKA_TYPE_ID) {
            success = execute_with_retry([&] {
                return handle_insert(static_cast<const KafkaInsertData&>(data));
            }, "kafka message insert");
        } else {
            throw std::runtime_error(
                "Unsupported data type for KafkaWriter: " +
                std::string(data.type.name())
            );
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