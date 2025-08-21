
#include "MqttClient.hpp"

#include <vector>
#include <string>
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <functional>
#include <memory>

class MockMqttClient : public IMqttClient {
public:
    bool connected = false;
    std::vector<std::string> published_topics;
    std::vector<std::string> published_payloads;
    std::vector<int> published_qos;
    std::vector<bool> published_retain;

    bool connect(const std::string& user, const std::string& password,
                int keep_alive, bool clean_session) override {
        (void)user; (void)password; (void)keep_alive; (void)clean_session;
        connected = true;
        return true;
    }

    bool is_connected() const override {
        return connected;
    }

    void disconnect() override {
        connected = false;
    }

    void publish(const std::string& topic, const std::string& payload,
                int qos, bool retain, const std::string& content_type,
                const std::string& compression, const std::string& encoding) override {
        (void)content_type; (void)compression; (void)encoding;
        published_topics.push_back(topic);
        published_payloads.push_back(payload);
        published_qos.push_back(qos);
        published_retain.push_back(retain);
    }
};

MqttInfo create_test_mqtt_info() {
    MqttInfo info;
    info.host = "localhost";
    info.port = 1883;
    info.user = "user";
    info.password = "pass";
    info.client_id = "test_client";
    info.keep_alive = 60;
    info.clean_session = true;
    info.qos = 1;
    info.retain = false;
    info.compression = "none";
    info.encoding = "utf8";
    info.topic = "factory/{factory_id}/device-{device_id}/data";
    return info;
}

void test_connect_and_close() {
    auto info = create_test_mqtt_info();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    // Patch: replace client_ with mock
    MqttClient client(info, col_instances);
    client.set_client(std::make_unique<MockMqttClient>());

    assert(client.connect());
    assert(client.is_connected());
    client.close();
    assert(!client.is_connected());

    std::cout << "test_connect_and_close passed." << std::endl;
}

void test_select_db_and_prepare() {
    auto info = create_test_mqtt_info();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MqttClient client(info, col_instances);

    assert(client.select_db("db1"));
    assert(client.prepare("sql"));
    std::cout << "test_select_db_and_prepare passed." << std::endl;
}

void test_execute_and_publish() {
    auto info = create_test_mqtt_info();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MqttClient client(info, col_instances);
    auto mock = std::make_unique<MockMqttClient>();
    auto* mock_ptr = mock.get();
    client.set_client(std::move(mock));

    // Prepare mock data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {"f01", "d01"}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    StmtV2InsertData stmt(block, col_instances);

    assert(client.connect());
    bool ok = client.execute(stmt);
    (void)ok;
    assert(ok);

    // Check published topic and payload
    (void)mock_ptr;
    assert(mock_ptr->published_topics.size() == 1);
    assert(mock_ptr->published_topics[0] == "factory/f01/device-d01/data");
    assert(mock_ptr->published_payloads[0].find("\"factory_id\":\"f01\"") != std::string::npos);
    assert(mock_ptr->published_payloads[0].find("\"device_id\":\"d01\"") != std::string::npos);

    std::cout << "test_execute_and_publish passed." << std::endl;
}

void test_execute_not_connected() {
    auto info = create_test_mqtt_info();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MqttClient client(info, col_instances);
    client.set_client(std::make_unique<MockMqttClient>());

    // Prepare mock data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {"f01", "d01"}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    StmtV2InsertData stmt(block, col_instances);

    // Not connected
    auto connected = client.is_connected();
    (void)connected;
    assert(!connected);

    auto execute_result = client.execute(stmt);
    (void)execute_result;
    assert(!execute_result);

    std::cout << "test_execute_not_connected passed." << std::endl;
}

void test_serialize_row_to_json() {
    auto info = create_test_mqtt_info();
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"factory_id", "VARCHAR(16)"});
    col_instances.emplace_back(ColumnConfig{"device_id", "VARCHAR(16)"});

    MqttClient client(info, col_instances);
    client.set_client(std::make_unique<MockMqttClient>());

    // Prepare mock data
    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {"f01", "d01"}});
    batch.table_batches.emplace_back("tb1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 1, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    StmtV2InsertData stmt(block, col_instances);

    // Serialize to JSON
    auto json = client.serialize_row_to_json(block->tables[0], 0);
    assert(json["timestamp"] == 1500000000000);
    assert(json["table"] == "tb1");
    assert(json["factory_id"] == "f01");
    assert(json["device_id"] == "d01");

    std::cout << "test_serialize_row_to_json passed." << std::endl;
}

int main() {
    test_connect_and_close();
    test_select_db_and_prepare();
    test_execute_and_publish();
    test_execute_not_connected();
    test_serialize_row_to_json();
    std::cout << "All MqttClient tests passed." << std::endl;
    return 0;
}