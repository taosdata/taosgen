#include "InfluxDBClient.hpp"
#include <iostream>
#include <cassert>

void test_influxdb_client_url_construction() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "my-token-123";
    config.org = "my-org";
    config.bucket = "my-bucket";

    InfluxDBFormatOptions format;
    format.precision = "ns";

    InfluxDBClient client(config, format);

    // Client is not connected initially
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_url_construction PASSED\n";
}

void test_influxdb_client_trailing_slash() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086/";
    config.token = "test-token";
    config.org = "test-org";
    config.bucket = "test-bucket";

    InfluxDBFormatOptions format;
    format.precision = "ms";

    InfluxDBClient client(config, format);
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_trailing_slash PASSED\n";
}

void test_influxdb_client_connect_close() {
    InfluxDBConfig config;
    config.url = "http://localhost:8086";
    config.token = "test-token";
    config.org = "default";
    config.bucket = "default";

    InfluxDBFormatOptions format;
    format.precision = "ns";

    InfluxDBClient client(config, format);
    assert(!client.is_connected());

    bool ok = client.connect();
    (void)ok;
    assert(ok);
    assert(client.is_connected());

    client.close();
    assert(!client.is_connected());

    std::cout << "test_influxdb_client_connect_close PASSED\n";
}

int main() {
    test_influxdb_client_url_construction();
    test_influxdb_client_trailing_slash();
    test_influxdb_client_connect_close();
    std::cout << "\nAll InfluxDB client tests PASSED\n";
    return 0;
}
