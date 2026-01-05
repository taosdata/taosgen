#include "ConfigParser.hpp"
#include "MqttConfigParser.hpp"
#include "MqttRegistrar.hpp"
#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>

void test_Mqtt() {
    std::string yaml = R"(
uri: mqtt.example.com:1883
user: testuser
password: testpass
client_id: client-001
keep_alive: 60
clean_session: true
max_buffered_messages: 1000
)";
    YAML::Node node = YAML::Load(yaml);
    MqttConfig mqtt = node.as<MqttConfig>();
    assert(mqtt.uri == "mqtt.example.com:1883");
    assert(mqtt.user == "testuser");
    assert(mqtt.password == "testpass");
    assert(mqtt.client_id == "client-001");
    assert(mqtt.keep_alive == 60);
    assert(mqtt.clean_session == true);
    assert(mqtt.max_buffered_messages == 1000);
}

void test_InsertDataConfig_mqtt() {
    std::string yaml = R"(
target: mqtt
mqtt:
  uri: "tcp://localhost:1883"
  user: "user1"
schema:
  name: test_schema
  columns:
    - name: c1
      type: INT
format: json
topic: "test/topic/{tag1}"
compression: gzip
encoding: GBK
tbname_key: "table"
qos: 2
retain: true
records_per_message: 10
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig idc = node.as<InsertDataConfig>();
    auto* mc = get_plugin_config_mut<MqttConfig>(idc.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(idc.target_type == "mqtt");
    assert(mc->uri == "tcp://localhost:1883");
    assert(mc->user == "user1");
    assert(idc.schema.name == "test_schema");
    assert(idc.data_format.format_type == "mqtt");
    assert(idc.data_format.mqtt.content_type == "json");
    assert(idc.data_format.mqtt.topic == "test/topic/{tag1}");
    assert(idc.data_format.mqtt.compression == "gzip");
    assert(idc.data_format.mqtt.encoding == "GBK");
    assert(idc.data_format.mqtt.tbname_key == "table");
    assert(idc.data_format.mqtt.qos == 2);
    assert(idc.data_format.mqtt.retain == true);
    assert(idc.data_format.mqtt.records_per_message == 10);
}

int main() {
    register_mqtt_plugin_config_hooks();
    test_Mqtt();
    test_InsertDataConfig_mqtt();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}