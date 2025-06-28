#include <cassert>
#include <iostream>
#include "TDengineWriter.h"


InsertDataConfig create_test_config() {
    InsertDataConfig config;
    
    // Target config
    config.target.timestamp_precision = "ms";
    config.target.tdengine.connection_info = {
        .host = "localhost",
        .port = 6030, 
        .user = "root",
        .password = "taosdata",
        .dsn = std::nullopt
    };
    config.target.tdengine.database_info.name = "test_action_db"; 
    config.target.tdengine.super_table_info.name = "test_super_table";

    // Control config
    config.control.data_channel.channel_type = "native";
    config.control.data_format.format_type = "sql";
    
    return config;
}

void test_constructor() {
    // Test valid timestamp precision
    auto config = create_test_config();
    TDengineWriter writer(config);
    
    // Test empty timestamp precision (should default to "ms")
    config.target.timestamp_precision = "";
    TDengineWriter writer_empty_ts(config);
    
    // Test invalid timestamp precision
    config.target.timestamp_precision = "invalid";
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
    TDengineWriter writer(config);
    
    // Test successful connection
    bool connected = writer.connect();
    (void)connected;
    assert(connected);
    
    // Test reconnection (should return true if already connected)
    connected = writer.connect();
    assert(connected);
    
    // Test connection with invalid config
    config.target.tdengine.connection_info.host = "invalid_host";
    TDengineWriter invalid_writer(config);
    connected = invalid_writer.connect();
    assert(!connected);
    
    std::cout << "test_connection passed." << std::endl;
}

void test_write_operations() {
    auto config = create_test_config();
    TDengineWriter writer(config);
    assert(writer.connect());
    
    // Test write without connection
    TDengineWriter no_conn_writer(config);
    try {
        SqlInsertData data(0, 0, 0, "test");
        no_conn_writer.write(data);
        assert(false); // Should not reach here
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "TDengineWriter is not connected");
    }
    
    // Test SQL write
    SqlInsertData sql_data(1000, 2000, 1, "INSERT INTO `test_action_db`.`d0` VALUES (1700000000000, 105, 3.1415926)");
    writer.write(sql_data);
    
    // Test STMT write
    ColumnConfigInstanceVector col_instances;
    col_instances.emplace_back(ColumnConfig{"col1", "INT"});
    col_instances.emplace_back(ColumnConfig{"col2", "DOUBLE"});

    std::string sql = "INSERT INTO `"
        + config.target.tdengine.database_info.name + "`.`" 
        + config.target.tdengine.super_table_info.name + "`(tbname,ts,col1,col2) VALUES(?,?,?,?)";

    assert (writer.prepare(sql));

    MultiBatch batch;
    std::vector<RowData> rows;
    rows.push_back({1500000010000, {10000, 108.00}});
    rows.push_back({1500000010001, {10001, 108.01}});
    batch.table_batches.emplace_back("d2", std::move(rows));
    batch.start_time = 1500000010000;
    batch.end_time = 1500000010001;
    batch.total_rows = 2;

    StmtV2InsertData stmt_data(
        batch.start_time,
        batch.end_time,
        batch.total_rows,
        col_instances,
        std::move(batch)
    );
    writer.write(stmt_data);

    // Test unsupported data type
    BaseInsertData invalid_data(static_cast<BaseInsertData::DataType>(999), 1000, 2000, 1);

    try {
        writer.write(invalid_data);
        assert(false); // Should not reach here
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()) == "Unsupported data type");
    }

    std::cout << "test_write_operations passed." << std::endl;
}

void test_retry_mechanism() {
    auto config = create_test_config();
    TDengineWriter writer(config);
    assert(writer.connect());
    
    // Test successful write after retry
    SqlInsertData data(1000, 2000, 1, "INSERT INTO `test_action_db`.`d5` VALUES (1700000000001, 105, 3.1415926)");
    writer.write(data);
    
    std::cout << "test_retry_mechanism passed." << std::endl;
}

int main() {
    test_constructor();
    test_connection();
    test_write_operations();
    test_retry_mechanism();
    
    std::cout << "All TDengineWriter tests passed." << std::endl;
    return 0;
}