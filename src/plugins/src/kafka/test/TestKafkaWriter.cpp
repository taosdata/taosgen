#include "KafkaWriter.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaClient.hpp"

#include <cassert>
#include <iostream>
#include <optional>
#include <memory>
#include <stdexcept>

// Mock implementation of the IKafkaClient interface for testing purposes.
class MockKafkaClient : public IKafkaClient {
public:
    bool connected = false;
    size_t produce_count = 0;
    size_t total_rows_produced = 0;
    bool fail_connect = false;
    int fail_produce_times = 0;

    bool connect() override {
        if (fail_connect) {
            return false;
        }
        connected = true;
        return true;
    }

    bool is_connected() const override {
        return connected;
    }

    void close() override {
        connected = false;
    }

    bool produce(const KafkaInsertData& data) override {
        produce_count++;

        if (fail_produce_times > 0) {
            fail_produce_times--;
            return false;
        }

        total_rows_produced += data.total_rows;
        return true;
    }
};

KafkaConfig* get_kafka_config(InsertDataConfig& config) {
    return get_plugin_config_mut<KafkaConfig>(config.extensions, "kafka");
}

KafkaFormatOptions* get_kafka_format_options(InsertDataConfig& config) {
    return get_format_opt_mut<KafkaFormatOptions>(config.data_format, "kafka");
}

// Creates a test configuration for the Kafka writer.
InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Connector Config
    set_plugin_config(config.extensions, "kafka", KafkaConfig{});
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto& conn_conf = *kc;
    conn_conf.bootstrap_servers = "localhost:9092";
    conn_conf.topic = "test_topic";
    conn_conf.client_id = "test_client";

    // Format Config
    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});
    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto& format_conf = *kf;
    format_conf.key_pattern = "{device_id}";
    format_conf.key_serializer = "string-utf8";
    format_conf.value_serializer = "json";
    format_conf.tbname_key = "table";
    format_conf.acks = "1";
    format_conf.compression = "none";
    format_conf.records_per_message = 1;

    // Failure Handling
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    return config;
}

// Creates a vector of column config instances for testing.
ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});
    return col_instances;
}

void test_create_kafka_writer() {
    InsertDataConfig config;
    config.target_type = "kafka";
    set_plugin_config(config.extensions, "kafka", KafkaConfig{});

    config.data_format.format_type = "kafka";
    set_format_opt(config.data_format, "kafka", KafkaFormatOptions{});

    auto writer = WriterFactory::create_writer(config);
    assert(writer != nullptr);

    // Verify the created type
    auto* kafka_writer = dynamic_cast<KafkaWriter*>(writer.get());
    assert(kafka_writer != nullptr);
    (void)kafka_writer;

    std::cout << "test_create_kafka_writer passed." << std::endl;
}

void test_constructor() {
    auto config = create_test_config();
    KafkaWriter writer(config);
    std::cout << "test_constructor passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    KafkaWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    writer.set_client(std::move(kafka_client));

    std::optional<ConnectorSource> conn_src;
    assert(writer.connect(conn_src));
    assert(mock_ptr->is_connected());
    (void)mock_ptr;
    (void)conn_src;

    // Connect again if already connected
    assert(writer.connect(conn_src));
    assert(mock_ptr->is_connected());

    // Disconnect
    writer.close();

    std::cout << "test_connection passed." << std::endl;
}

void test_connection_failure() {
    auto config = create_test_config();
    KafkaWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_connect = true;
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    writer.set_client(std::move(kafka_client));

    std::optional<ConnectorSource> conn_src;
    assert(!writer.connect(conn_src));
    assert(!mock_ptr->is_connected());
    (void)mock_ptr;
    (void)conn_src;

    std::cout << "test_connection_failure passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};
    KafkaWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    writer.set_client(std::move(kafka_client));

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Construct kafka data
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        KafkaMessageBatch msg_batch;
        msg_batch.emplace_back("d01", R"({"device_id":"d01","factory_id":"f01"})");
        KafkaInsertData msg(block, col_instances, tag_instances, std::move(msg_batch));

        writer.write(msg);
        (void)mock_ptr;
        assert(mock_ptr->produce_count == 1);
        assert(mock_ptr->total_rows_produced == 1);
    }

    // Unsupported data type
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb2", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));
        BaseInsertData invalid_data(typeid(void), block, col_instances, tag_instances);

        try {
            writer.write(invalid_data);
            assert(false);
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
        }
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_with_retry() {
    auto config = create_test_config();
    config.failure_handling.max_retries = 1;
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};
    KafkaWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockKafkaClient>();
    mock->fail_produce_times = 1; // Fail once
    auto* mock_ptr = mock.get();
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::move(mock));
    writer.set_client(std::move(kafka_client));

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    KafkaMessageBatch msg_batch;
    msg_batch.emplace_back("d01", R"({"device_id":"d01","factory_id":"f01"})");
    KafkaInsertData msg(block, col_instances, tag_instances, std::move(msg_batch));

    assert(writer.write(msg));
    assert(mock_ptr->produce_count == 2);           // Called twice: 1 fail + 1 success
    assert(mock_ptr->total_rows_produced == 1);     // Only succeeded once

    (void)mock_ptr;
    (void)conn_src;

    std::cout << "test_write_with_retry passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};
    KafkaWriter writer(config);

    // Replace with mock
    auto* kc = get_kafka_config(config);
    assert(kc != nullptr);

    auto* kf = get_kafka_format_options(config);
    assert(kf != nullptr);

    auto kafka_client = std::make_unique<KafkaClient>(*kc, *kf);
    kafka_client->set_client(std::make_unique<MockKafkaClient>());
    writer.set_client(std::move(kafka_client));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {std::string("f0"), std::string("d0")}});
    rows.push_back({1500000010001, {std::string("f1"), std::string("d1")}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    KafkaInsertData data(block, col_instances, tag_instances, {});

    try {
        writer.write(data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "KafkaWriter is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

int main() {
    test_create_kafka_writer();
    test_constructor();
    test_connection();
    test_connection_failure();
    test_write_operations();
    test_write_with_retry();
    test_write_without_connection();
    std::cout << "All KafkaWriter tests passed." << std::endl;
    return 0;
}