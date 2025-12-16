#include "WriterFactory.hpp"
#include "TDengineWriter.hpp"
#include "MqttWriter.hpp"
#include "KafkaWriter.hpp"
#include "InsertDataConfig.hpp"
#include "ColumnConfigInstance.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

// Creates a basic InsertDataConfig for a given target type.
InsertDataConfig create_test_config(const std::string& target_type) {
    InsertDataConfig config;
    config.target_type = target_type;
    return config;
}

void test_create_tdengine_writer() {
    auto config = create_test_config("tdengine");
    auto writer = WriterFactory::create(config);
    assert(writer != nullptr);

    // Verify the created type
    auto* td_writer = dynamic_cast<TDengineWriter*>(writer.get());
    assert(td_writer != nullptr);
    (void)td_writer;

    std::cout << "test_create_tdengine_writer passed." << std::endl;
}

void test_create_mqtt_writer() {
    auto config = create_test_config("mqtt");
    auto writer = WriterFactory::create(config);
    assert(writer != nullptr);

    // Verify the created type
    auto* mqtt_writer = dynamic_cast<MqttWriter*>(writer.get());
    assert(mqtt_writer != nullptr);
    (void)mqtt_writer;

    std::cout << "test_create_mqtt_writer passed." << std::endl;
}

void test_create_kafka_writer() {
    auto config = create_test_config("kafka");
    auto writer = WriterFactory::create(config);
    assert(writer != nullptr);

    // Verify the created type
    auto* kafka_writer = dynamic_cast<KafkaWriter*>(writer.get());
    assert(kafka_writer != nullptr);
    (void)kafka_writer;

    std::cout << "test_create_kafka_writer passed." << std::endl;
}

void test_create_unsupported_writer() {
    std::string unsupported_type = "non_existent_db";
    auto config = create_test_config(unsupported_type);

    try {
        auto writer = WriterFactory::create(config);
        assert(false); // Should not reach here
    } catch (const std::invalid_argument& e) {
        std::string expected_msg = "Unsupported target type: " + unsupported_type;
        assert(std::string(e.what()) == expected_msg);
    }

    std::cout << "test_create_unsupported_writer passed." << std::endl;
}

void test_create_csv_writer_throws() {
    auto config = create_test_config("csv");

    try {
        auto writer = WriterFactory::create(config);
        assert(false); // Should not reach here
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Unsupported target type: csv");
    }

    std::cout << "test_create_csv_writer_throws passed." << std::endl;
}

int main() {
    test_create_tdengine_writer();
    test_create_mqtt_writer();
    test_create_kafka_writer();
    test_create_unsupported_writer();
    test_create_csv_writer_throws();

    std::cout << "All WriterFactory tests passed." << std::endl;
    return 0;
}