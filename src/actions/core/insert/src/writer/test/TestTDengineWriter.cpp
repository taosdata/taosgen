#include "TDengineWriter.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <optional>

InsertDataConfig create_test_config() {
    InsertDataConfig config;

    // Source config
    config.schema.tbname.source_type = "generator";
    config.schema.columns = {
        ColumnConfig{"col1", "INT"},
        ColumnConfig{"col2", "DOUBLE"}
    };

    // Target config
    config.timestamp_precision = "ms";
    config.tdengine = TDengineConfig{
        "taos://root:taosdata@localhost:6030/test_action"
    };
    config.schema.name = "test_super_table";
    config.schema.apply();

    // Control config
    config.data_format.format_type = "sql";

    // Insert control for retry
    config.failure_handling.max_retries = 1;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";

    return config;
}

void test_constructor() {
    // Test valid timestamp precision
    auto config = create_test_config();
    TDengineWriter writer(config);

    // Test empty timestamp precision (should default to "ms")
    config.timestamp_precision = "";
    TDengineWriter writer_empty_ts(config);

    // Test invalid timestamp precision
    config.timestamp_precision = "invalid";
    try {
        TDengineWriter writer_invalid(config);
        assert(false); // Should not reach here
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()) == "Invalid timestamp precision: invalid");
    }

    std::cout << "test_constructor passed." << std::endl;
}

void test_connection() {
    auto config = create_test_config();

    std::optional<ConnectorSource> conn_src;
    TDengineWriter writer(config);

    // Test successful connection
    bool connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Test reconnection (should return true if already connected)
    connected = writer.connect(conn_src);
    assert(connected);

    // Test connection with invalid config
    config.tdengine.host = "invalid_host";
    TDengineWriter invalid_writer(config);
    connected = invalid_writer.connect(conn_src);
    assert(!connected);

    std::cout << "test_connection passed." << std::endl;
}

void test_select_db_and_prepare() {
    auto config = create_test_config();
    TDengineWriter writer(config);

    // Not connected, should throw
    try {
        writer.select_db("information_schema");
        assert(false);
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineWriter is not connected");
    }

    std::string sql = "SELECT * FROM `information_schema`.`ins_dnodes` where id=?";

    try {
        writer.prepare(sql);
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
    auto prep_ok = writer.prepare(sql);
    (void)db_ok;
    (void)prep_ok;
    assert(db_ok);
    assert(prep_ok);

    std::cout << "test_select_db_and_prepare passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());

    std::optional<ConnectorSource> conn_src;
    TDengineWriter writer(config);
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    // Test SQL write
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d0", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        SqlInsertData sql_data(block, col_instances, tag_instances, "INSERT INTO `test_action`.`d0` VALUES (1700000000000, 105, 3.1415926)");
        writer.write(sql_data);
    }

    // Test STMT write
    {
        std::string sql = "INSERT INTO `"
            + config.tdengine.database + "`.`"
            + config.schema.name + "`(tbname,ts,col1,col2) VALUES(?,?,?,?)";

        auto prepared = writer.prepare(sql);
        (void)prepared;
        assert(prepared);

        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d1", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        StmtV2InsertData stmt_data(block, col_instances, tag_instances);
        writer.write(stmt_data);
    }

    // Test unsupported data type
    {
        MultiBatch batch;
        std::vector<RowData> rows;
        rows.push_back({1500000010000, {10000, 108.00}});
        rows.push_back({1500000010001, {10001, 108.01}});
        batch.table_batches.emplace_back("d2", std::move(rows));
        batch.update_metadata();

        MemoryPool pool(1, 1, 2, col_instances, tag_instances);
        auto* block = pool.convert_to_memory_block(std::move(batch));

        BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(999), block, col_instances, tag_instances);

        try {
            writer.write(invalid_data);
            assert(false); // Should not reach here
        } catch (const std::runtime_error& e) {
            assert(std::string(e.what()).find("Unsupported data type") != std::string::npos);
        }
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_write_without_connection() {
    auto config = create_test_config();
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineWriter writer(config);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertData sql_data(block, col_instances, tag_instances, "test");
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
    config.failure_handling.max_retries = 2;
    config.failure_handling.retry_interval_ms = 1;
    config.failure_handling.on_failure = "exit";
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineWriter writer(config);

    std::optional<ConnectorSource> conn_src;
    auto connected = writer.connect(conn_src);
    (void)connected;
    assert(connected);

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000000000, {42, 3.14}});
    rows.push_back({1500000000001, {43, 2.71}});
    batch.table_batches.emplace_back("table1", std::move(rows));
    batch.update_metadata();

    MemoryPool pool(1, 1, 2, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));

    SqlInsertData sql_data(block, col_instances, tag_instances, "show databases");
    writer.write(sql_data);

    std::cout << "test_retry_mechanism skipped (not implemented)." << std::endl;
}

void test_metrics_and_time() {
    auto config = create_test_config();
    TDengineWriter writer(config);

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