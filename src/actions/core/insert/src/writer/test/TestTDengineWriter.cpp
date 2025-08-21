#include "TDengineWriter.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <optional>

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Source config
    config.source.table_name.source_type = "generator";
    config.source.columns.source_type = "generator";
    config.source.columns.generator.schema = {
        ColumnConfig{"col1", "INT"},
        ColumnConfig{"col2", "DOUBLE"}
    };

    // Target config
    config.target.timestamp_precision = "ms";
    config.target.tdengine.connection_info = ConnectionInfo{
        "localhost",
        6030,
        "root",
        "taosdata"
    };
    config.target.tdengine.database_info.name = "test_action_db";
    config.target.tdengine.super_table_info.name = "test_super_table";

    // Control config
    config.control.data_channel.channel_type = "native";
    config.control.data_format.format_type = "sql";

    // Insert control for retry
    config.control.insert_control.failure_handling.max_retries = 1;
    config.control.insert_control.failure_handling.retry_interval_ms = 1;
    config.control.insert_control.failure_handling.on_failure = "exit";

    return config;
}

void test_constructor() {
    // Test valid timestamp precision
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());
    TDengineWriter writer(config, col_instances);

    // Test empty timestamp precision (should default to "ms")
    config.target.timestamp_precision = "";
    TDengineWriter writer_empty_ts(config, col_instances);

    // Test invalid timestamp precision
    config.target.timestamp_precision = "invalid";
    try {
        TDengineWriter writer_invalid(config, col_instances);
        assert(false); // Should not reach here
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Invalid timestamp precision: invalid");
    }

    std::cout << "test_constructor passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());

    std::optional<ConnectorSource> conn_src;
    TDengineWriter writer(config, col_instances);

    // Test successful connection
    bool connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Test reconnection (should return true if already connected)
    connected = writer.connect(conn_src);
    assert(connected);

    // Test connection with invalid config
    config.target.tdengine.connection_info.host = "invalid_host";
    TDengineWriter invalid_writer(config, col_instances);
    connected = invalid_writer.connect(conn_src);
    assert(!connected);

    std::cout << "test_connection passed." << std::endl;
}

void test_select_db_and_prepare() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());
    TDengineWriter writer(config, col_instances);

    // Not connected, should throw
    try {
        writer.select_db("information_schema");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineWriter is not connected");
    }
    try {
        writer.prepare("sql");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineWriter is not connected");
    }

    // Connect and test select_db/prepare
    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    auto db_ok = writer.select_db("information_schema");
    auto prep_ok = writer.prepare("sql");
    (void)db_ok;
    (void)prep_ok;
    assert(db_ok);
    assert(prep_ok);

    std::cout << "test_select_db_and_prepare passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());

    std::optional<ConnectorSource> conn_src;
    TDengineWriter writer(config, col_instances);
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);


    // Test SQL write
    SqlInsertData sql_data(1000, 2000, 1, "INSERT INTO `test_action_db`.`d0` VALUES (1700000000000, 105, 3.1415926)");
    writer.write(sql_data);

    // Test STMT write
    std::string sql = "INSERT INTO `"
        + config.target.tdengine.database_info.name + "`.`"
        + config.target.tdengine.super_table_info.name + "`(tbname,ts,col1,col2) VALUES(?,?,?,?)";

    auto prepared = writer.prepare(sql);
    (void)prepared;
    assert(prepared);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {10000, 108.00}});
    rows.push_back({1500000010001, {10001, 108.01}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    StmtV2InsertData stmt_data(block, col_instances);
    writer.write(stmt_data);

    // Test unsupported data type
    BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(999), 1000, 2000, 1);

    try {
        writer.write(invalid_data);
        assert(false); // Should not reach here
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());
    TDengineWriter writer(config, col_instances);

    SqlInsertData sql_data(0, 0, 0, "test");
    try {
        writer.write(sql_data);
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineWriter is not connected");
    }

    std::cout << "test_write_without_connection passed." << std::endl;
}

void test_retry_mechanism() {
    auto config = create_test_config();
    config.control.insert_control.failure_handling.max_retries = 2;
    config.control.insert_control.failure_handling.retry_interval_ms = 1;
    config.control.insert_control.failure_handling.on_failure = "exit";
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());
    TDengineWriter writer(config, col_instances);

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);
    SqlInsertData sql_data(1000, 2000, 1, "show databases");
    writer.write(sql_data);

    std::cout << "test_retry_mechanism skipped (not implemented)." << std::endl;
}

void test_metrics_and_time() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.source.columns.get_schema());
    TDengineWriter writer(config, col_instances);

    assert(writer.get_timestamp_precision() == "ms");
    const ActionMetrics& play = writer.get_play_metrics();
    const ActionMetrics& write = writer.get_write_metrics();
    (void)play;
    (void)write;
    auto start = writer.start_write_time();
    auto end = writer.end_write_time();
    (void)start;
    (void)end;
    assert(end >= start);

    std::cout << "test_metrics_and_time passed." << std::endl;
}

int main() {
    test_constructor();
    test_connection();
    test_select_db_and_prepare();
    test_write_operations();
    test_write_without_connection();
    test_retry_mechanism();
    test_metrics_and_time();

    std::cout << "All TDengineWriter tests passed." << std::endl;
    return 0;
}