#include "MqttWriter.hpp"
#include "MqttInsertData.hpp"
#include "MqttClient.hpp"

#include <cassert>
#include <iostream>
#include <optional>
#include <memory>
#include <stdexcept>

class MockMqttClient : public IMqttClient {
public:
    bool connected = false;
    size_t publish_count = 0;
    size_t total_rows_published = 0;
    bool fail_connect = false;
    int fail_publish_times = 0;

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

    void disconnect() override {
        connected = false;
    }

    bool publish(const MqttInsertData& data) override {
        publish_count++;

        if (fail_publish_times > 0) {
            fail_publish_times--;
            return false;
        }

        total_rows_published += data.total_rows;
        return true;
    }
};


InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Connector Config
    auto& conn_conf = config.mqtt;
    conn_conf.uri = "tcp://localhost:1883";
    conn_conf.user = "user";
    conn_conf.password = "pass";
    conn_conf.client_id = "test_client";
    conn_conf.keep_alive = 60;
    conn_conf.clean_session = true;

    // Format Config
    auto& format_conf = config.data_format.mqtt;
    format_conf.topic = "factory/{factory_id}/device-{device_id}/data";
    format_conf.qos = 1;
    format_conf.retain = false;
    format_conf.compression = "none";
    format_conf.encoding = "utf8";
    format_conf.content_type = "json";
    format_conf.records_per_message = 10;

    // Failure Handling
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    return config;
}

ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});
    return col_instances;
}

void test_create_mqtt_writer() {
    InsertDataConfig config;
    config.target_type = "mqtt";

    auto writer = WriterFactory::create_writer(config);
    assert(writer != nullptr);

    // Verify the created type
    auto* mqtt_writer = dynamic_cast<MqttWriter*>(writer.get());
    assert(mqtt_writer != nullptr);
    (void)mqtt_writer;

    std::cout << "test_create_mqtt_writer passed." << std::endl;
}

void test_constructor() {
    auto config = create_test_config();
    MqttWriter writer(config);
    std::cout << "test_constructor passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    MqttWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

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
    MqttWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_connect = true;
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

    std::optional<ConnectorSource> conn_src;
    assert(!writer.connect(conn_src));
    assert(!mock_ptr->is_connected());
    (void)mock_ptr;
    (void)conn_src;

    std::cout << "test_connection_failure passed." << std::endl;
}

void test_prepare() {
    auto config = create_test_config();
    MqttWriter writer(config);

    // Replace with mock
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::make_unique<MockMqttClient>());
    writer.set_client(std::move(mqtt_client));

    // prepare should throw when not connected
    try {
        writer.prepare("context");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttWriter is not connected");
    }

    // After connecting, should be available
    std::optional<ConnectorSource> conn_src;
    assert(writer.connect(conn_src));
    assert(writer.prepare("context"));

    std::cout << "test_prepare passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};
    MqttWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Construct mqtt data
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000000000, {std::string("f01"), std::string("d01")}});
        batch.table_batches.emplace_back("tb1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 1, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        // Create a simple message batch for the test
        MqttMessageBatch msg_batch;
        msg_batch.emplace_back("test/topic", "{\"factory_id\":\"f01\", \"device_id\":\"d01\"}");
        MqttInsertData msg(block, col_instances, tag_instances, std::move(msg_batch));

        writer.write(msg);
        (void)mock_ptr;
        assert(mock_ptr->publish_count == 1);
        assert(mock_ptr->total_rows_published == 1);
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
        BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(999), block, col_instances, tag_instances);

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
    MqttWriter writer(config);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    mock->fail_publish_times = 1; // Fail once
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

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

    MqttMessageBatch msg_batch;
    msg_batch.emplace_back("test/topic", "{\"factory_id\":\"f01\", \"device_id\":\"d01\"}");
    MqttInsertData msg(block, col_instances, tag_instances, std::move(msg_batch));

    assert(writer.write(msg));
    assert(mock_ptr->publish_count == 2);           // Called twice: 1 fail + 1 success
    assert(mock_ptr->total_rows_published == 1);    // Only succeeded once

    (void)mock_ptr;
    (void)conn_src;

    std::cout << "test_write_with_retry passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    auto tag_instances = ColumnConfigInstanceVector{};
    MqttWriter writer(config);

    // Replace with mock
    auto mqtt_client = std::make_unique<MqttClient>(config.mqtt, config.data_format.mqtt);
    mqtt_client->set_client(std::make_unique<MockMqttClient>());
    writer.set_client(std::move(mqtt_client));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {std::string("f0"), std::string("d0")}});
    rows.push_back({1500000010001, {std::string("f1"), std::string("d1")}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    MqttInsertData data(block, col_instances, tag_instances, {});

    try {
        writer.write(data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttWriter is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

int main() {
    test_create_mqtt_writer();
    test_constructor();
    test_connection();
    test_connection_failure();
    test_prepare();
    test_write_operations();
    test_write_with_retry();
    test_write_without_connection();
    std::cout << "All MqttWriter tests passed." << std::endl;
    return 0;
}