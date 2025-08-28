#include "MqttWriter.hpp"
#include "MsgInsertDataFormatter.hpp"
#include "MqttClient.hpp"
#include <cassert>
#include <iostream>
#include <optional>
#include <memory>

class MockMqttClient : public IMqttClient {
public:
    bool connected = false;
    size_t publish_count = 0;

    bool connect(const std::string&, const std::string&, int, bool) override {
        connected = true;
        return true;
    }
    bool is_connected() const override { return connected; }
    void disconnect() override { connected = false; }
    void publish(const std::string&, const std::string&, int, bool) override {
        ++publish_count;
    }
    void publish_batch(const MessageBatch& batch_msgs, int, bool) override {
        publish_count += batch_msgs.size();
    }
};

InsertDataConfig create_test_config() {
    InsertDataConfig config;
    config.target.mqtt.host = "localhost";
    config.target.mqtt.port = 1883;
    config.target.mqtt.user = "user";
    config.target.mqtt.password = "pass";
    config.target.mqtt.client_id = "test_client";
    config.target.mqtt.keep_alive = 60;
    config.target.mqtt.clean_session = true;
    config.target.mqtt.qos = 1;
    config.target.mqtt.retain = false;
    config.target.mqtt.compression = "none";
    config.target.mqtt.encoding = "utf8";
    config.target.mqtt.topic = "factory/{factory_id}/device-{device_id}/data";
    config.control.insert_control.failure_handling.max_retries = 1;
    config.control.insert_control.failure_handling.retry_interval_ms = 1;
    config.control.insert_control.failure_handling.on_failure = "exit";
    return config;
}

ColumnConfigInstanceVector create_col_instances() {
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});
    return col_instances;
}

void test_constructor() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    MqttWriter writer(config, col_instances);
    std::cout << "test_constructor passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    MqttWriter writer(config, col_instances);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.target.mqtt, col_instances);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

    std::optional<ConnectorSource> conn_src;
    assert(writer.connect(conn_src));
    assert(mock_ptr->is_connected());
    (void)mock_ptr;

    // Connect again if already connected
    assert(writer.connect(conn_src));

    // Disconnect
    writer.close();

    std::cout << "test_connection passed." << std::endl;
}

void test_select_db_and_prepare() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    MqttWriter writer(config, col_instances);

    // Replace with mock
    auto mqtt_client = std::make_unique<MqttClient>(config.target.mqtt, col_instances);
    mqtt_client->set_client(std::make_unique<MockMqttClient>());
    writer.set_client(std::move(mqtt_client));

    // select_db/prepare should throw when not connected
    try {
        writer.select_db("db1");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttWriter is not connected");
    }
    try {
        writer.prepare("sql");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttWriter is not connected");
    }

    // After connecting, should be available
    std::optional<ConnectorSource> conn_src;
    assert(writer.connect(conn_src));
    assert(writer.select_db("db1"));
    assert(writer.prepare("sql"));

    std::cout << "test_select_db_and_prepare passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    MqttWriter writer(config, col_instances);

    // Replace with mock
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    auto mqtt_client = std::make_unique<MqttClient>(config.target.mqtt, col_instances);
    mqtt_client->set_client(std::move(mock));
    writer.set_client(std::move(mqtt_client));

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Construct STMT data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {"f01", "d01"}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    MsgInsertData msg = MsgInsertDataFormatter::format_mqtt(config.target.mqtt, col_instances, block);

    writer.write(msg);
    (void)mock_ptr;
    assert(mock_ptr->publish_count == 1);

    // Unsupported data type
    BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(999), 0, 0, 0);
    try {
        writer.write(invalid_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = create_col_instances();
    MqttWriter writer(config, col_instances);

    // Replace with mock
    auto mqtt_client = std::make_unique<MqttClient>(config.target.mqtt, col_instances);
    mqtt_client->set_client(std::make_unique<MockMqttClient>());
    writer.set_client(std::move(mqtt_client));

    BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(1), 0, 0, 0);
    try {
        writer.write(invalid_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "MqttWriter is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

int main() {
    test_constructor();
    test_connection();
    test_select_db_and_prepare();
    test_write_operations();
    test_write_without_connection();
    std::cout << "All MqttWriter tests passed." << std::endl;
    return 0;
}