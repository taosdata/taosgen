#include "ParameterContext.hpp"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <vector>


// Test command line parameter parsing
void test_commandline_merge() {
    ParameterContext ctx;
    const char* argv[] = {
        "dummy_program",
        "--host=127.0.0.1",
        "--port=6041",
        "--user=admin",
        "--password=taosdata"
    };
    ctx.merge_commandline(5, const_cast<char**>(argv));

    const auto& conn_info = ctx.get_connection_info();
    (void)conn_info;
    assert(conn_info.host == "127.0.0.1");
    assert(conn_info.port == 6041);
    assert(conn_info.user == "admin");
    assert(conn_info.password == "taosdata");
    std::cout << "Commandline merge test passed.\n";
}

// Test environment variable merge
void test_environment_merge() {
    ParameterContext ctx;
    setenv("TAOS_HOST", "192.168.1.100", 1);
    setenv("TAOS_PORT", "6042", 1);
    ctx.merge_environment_vars();

    const auto& conn_info = ctx.get_connection_info();
    (void)conn_info;
    assert(conn_info.host == "192.168.1.100");
    assert(conn_info.port == 6042);
    std::cout << "Environment merge test passed.\n";
}

// Test YAML config merge
void test_yaml_merge() {
    ParameterContext ctx;

    // Simulate YAML config
    YAML::Node config = YAML::Load(R"(
tdengine:
  dsn: taos://root:secret@10.0.0.1:6043/testdb
  drop_if_exists: true
  props: vgroups 20 replica 3 keep 3650

schema:
  name: points
  tbname:
    prefix: d
    count: 100000
    from: 0
  columns: &columns_info
    - name: latitude
      type: float
    - name: longitude
      type: float
    - name: quality
      type: varchar(50)
  tags: &tags_info
    - name: type
      type: varchar(7)
    - name: name
      type: varchar(20)
    - name: department
      type: varchar(7)
  generation:
    interlace: 60
    per_table_rows: 10000
    per_batch_rows: 10000

global:
  connection_info: &db_conn
    dsn: "taos://root:secret@10.0.0.1:6043/tsbench"
    drop_if_exists: true
  database_info: &db_info
    name: testdb
    drop_if_exists: true
    precision: us
    props: vgroups 20 replica 3 keep 3650
  super_table_info: &stb_info
    name: points
    columns: &columns_info
      - name: latitude
        type: float
      - name: longitude
        type: float
      - name: quality
        type: varchar(50)
    tags: &tags_info
      - name: type
        type: varchar(7)
      - name: name
        type: varchar(20)
      - name: department
        type: varchar(7)

concurrency: 4
jobs:
  create-database:
    name: Create Database
    needs: []
    steps:
      - name: Create Database
        uses: tdengine/create-database

  create-super-table:
    name: Create Super Table
    needs: [create-database]
    steps:
      - name: Create Super Table
        uses: tdengine/create-super-table
        with:
          schema:
            name: points

  create-second-child-table:
    name: Create Second Child Table
    needs: [create-super-table]
    steps:
      - name: Create Second Child Table
        uses: actions/create-child-table
        with:
          schema:
            name: points
            from_csv:
              tags:
                file_path: /root/meta/cnnc_csv_1s.csv
                has_header: true
            tbname:
              prefix: s
              count: 10000
              from: 200
            batch:
              size: 1000
              concurrency: 10

  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: actions/insert-data
        with:
          # source
          source:
            table_name:
              prefix: s
              count: 10000
              from: 200
            columns:
              source_type: csv
              csv:
                schema: *columns_info
                file_path: /root/data/cnnc_csv_1s/
                has_header: true

                timestamp_strategy:
                  strategy_type: original
                  original:
                    column_index: 0
                    precision: us

          # target
          target:
            target_type: tdengine
            tdengine:
              connection_info: *db_conn
              database_info:
                name: testdb
                precision: us

              super_table_info:
                name: points
                columns: *columns_info
                tags: *tags_info

          # control
          control: &insert_second_control
            data_format:
              format_type: sql
            data_channel:
              channel_type: native
            data_generation:
              interlace_mode:
                enabled: true
                rows: 60
              generate_threads: 8
              per_table_rows: 10000
            insert_control:
              per_request_rows: 10000
              auto_create_table: false
              insert_threads: 8
              thread_allocation: vgroup_binding
            time_interval:
              enabled: true
              interval_strategy: first_to_first
)");

    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();

    // Validate global config
    assert(data.global.connection_info.host == "10.0.0.1");
    assert(data.global.connection_info.port == 6043);
    assert(data.concurrency == 4);

    // Validate job parsing
    // job: create-database
    assert(data.jobs.size() == 4);
    assert(data.jobs[0].key == "create-database");
    assert(data.jobs[0].name == "Create Database");
    assert(data.jobs[0].needs.size() == 0);
    assert(data.jobs[0].steps.size() == 1);
    assert(data.jobs[0].steps[0].name == "Create Database");
    assert(data.jobs[0].steps[0].uses == "tdengine/create-database");
    assert(std::holds_alternative<CreateDatabaseConfig>(data.jobs[0].steps[0].action_config));
    const auto& create_db_config = std::get<CreateDatabaseConfig>(data.jobs[0].steps[0].action_config);
    (void)create_db_config;
    assert(create_db_config.tdengine.host == "10.0.0.1");
    assert(create_db_config.tdengine.port == 6043);
    assert(create_db_config.tdengine.user == "root");
    assert(create_db_config.tdengine.password == "secret");
    assert(create_db_config.tdengine.database == "testdb");
    assert(create_db_config.tdengine.drop_if_exists == true);
    assert(create_db_config.tdengine.properties == "vgroups 20 replica 3 keep 3650");

    // job: create-super-table
    assert(data.jobs[1].key == "create-super-table");
    assert(data.jobs[1].name == "Create Super Table");
    assert(data.jobs[1].needs.size() == 1);
    assert(data.jobs[1].steps.size() == 1);
    assert(data.jobs[1].steps[0].name == "Create Super Table");
    assert(data.jobs[1].steps[0].uses == "tdengine/create-super-table");
    assert(std::holds_alternative<CreateSuperTableConfig>(data.jobs[1].steps[0].action_config));
    const auto& create_stb_config = std::get<CreateSuperTableConfig>(data.jobs[1].steps[0].action_config);
    (void)create_stb_config;
    assert(create_stb_config.tdengine.database == "testdb");
    assert(create_stb_config.schema.name == "points");
    assert(create_stb_config.schema.columns.size() > 0);
    assert(create_stb_config.schema.tags.size() > 0);

    // job: create-second-child-table
    assert(data.jobs[2].key == "create-second-child-table");
    assert(data.jobs[2].name == "Create Second Child Table");
    assert(data.jobs[2].needs.size() == 1);
    assert(data.jobs[2].steps.size() == 1);
    assert(data.jobs[2].steps[0].name == "Create Second Child Table");
    assert(data.jobs[2].steps[0].uses == "actions/create-child-table");
    assert(std::holds_alternative<CreateChildTableConfig>(data.jobs[2].steps[0].action_config));
    const auto& create_child_config = std::get<CreateChildTableConfig>(data.jobs[2].steps[0].action_config);
    (void)create_child_config;
    assert(create_child_config.tdengine.database == "testdb");
    assert(create_child_config.schema.name == "points");
    assert(create_child_config.schema.tbname.source_type == "generator");
    assert(create_child_config.schema.tbname.generator.prefix == "s");
    assert(create_child_config.schema.tbname.generator.count == 10000);
    assert(create_child_config.schema.tbname.generator.from == 200);
    // assert(create_child_config.schema.tags.source_type == "csv");
    // assert(create_child_config.schema.tags.csv.file_path == "/root/meta/cnnc_csv_1s.csv");
    // assert(create_child_config.schema.tags.csv.has_header == true);
    assert(create_child_config.batch.size == 1000);
    assert(create_child_config.batch.concurrency == 10);

    // job: insert-second-data
    assert(data.jobs[3].key == "insert-second-data");
    assert(data.jobs[3].name == "Insert Second-Level Data");
    assert(data.jobs[3].needs.size() == 1);
    assert(data.jobs[3].needs[0] == "create-second-child-table");
    assert(data.jobs[3].steps.size() == 1);
    assert(data.jobs[3].steps[0].name == "Insert Second-Level Data");
    assert(data.jobs[3].steps[0].uses == "actions/insert-data");
    assert(std::holds_alternative<InsertDataConfig>(data.jobs[3].steps[0].action_config));
    const auto& insert_config = std::get<InsertDataConfig>(data.jobs[3].steps[0].action_config);

    assert(insert_config.source.table_name.source_type == "generator");
    assert(insert_config.source.table_name.generator.prefix == "s");
    assert(insert_config.source.table_name.generator.count == 10000);
    assert(insert_config.source.table_name.generator.from == 200);

    assert(insert_config.source.columns.source_type == "csv");
    assert(insert_config.source.columns.csv.file_path == "/root/data/cnnc_csv_1s/");
    assert(insert_config.source.columns.csv.has_header == true);

    assert(insert_config.source.columns.csv.timestamp_strategy.strategy_type == "original");
    const auto& original_config = std::get<TimestampOriginalConfig>(
        insert_config.source.columns.csv.timestamp_strategy.timestamp_config);
    (void)original_config;
    assert(original_config.timestamp_index == 0);
    assert(original_config.timestamp_precision == "us");

    assert(insert_config.target.target_type == "tdengine");
    assert(insert_config.target.tdengine.connection_info.host == "10.0.0.1");
    assert(insert_config.target.tdengine.connection_info.port == 6043);
    assert(insert_config.target.tdengine.connection_info.user == "root");
    assert(insert_config.target.tdengine.connection_info.password == "secret");
    assert(insert_config.target.tdengine.database_info.name == "testdb");
    assert(insert_config.target.tdengine.database_info.precision == "us");
    assert(insert_config.target.tdengine.super_table_info.name == "points");
    assert(insert_config.target.tdengine.super_table_info.columns.size() > 0);
    assert(insert_config.target.tdengine.super_table_info.tags.size() > 0);

    assert(insert_config.control.data_format.format_type == "sql");
    assert(insert_config.control.data_channel.channel_type == "native");

    assert(insert_config.control.data_generation.interlace_mode.enabled == true);
    assert(insert_config.control.data_generation.interlace_mode.rows  == 60);
    assert(insert_config.control.data_generation.generate_threads == 8);
    assert(insert_config.control.data_generation.per_table_rows == 10000);

    assert(insert_config.control.insert_control.per_request_rows == 10000);
    assert(insert_config.control.insert_control.auto_create_table == false);
    assert(insert_config.control.insert_control.insert_threads == 8);
    assert(insert_config.control.insert_control.thread_allocation == "vgroup_binding");

    assert(insert_config.control.time_interval.enabled == true);
    assert(insert_config.control.time_interval.interval_strategy == "first_to_first");

    std::cout << "YAML merge test passed.\n";
}

// Test parameter priority (command line > environment variable > YAML)
void test_priority() {
    ParameterContext ctx;

    // Set environment variable
    setenv("TAOS_HOST", "env.host", 1);

    // Merge YAML
    YAML::Node config = YAML::Load(R"(
global:
  host: yaml.host
)");
    ctx.merge_yaml(config);

    // Merge command line parameters
    const char* argv[] = {"dummy", "--host=cli.host"};
    ctx.merge_commandline(2, const_cast<char**>(argv));

    // Validate priority
    assert(ctx.get_config_data().global.connection_info.host == "cli.host");
    std::cout << "Priority test passed.\n";
}

void test_unknown_key_detection() {
  YAML::Node config = YAML::Load(R"(
global:
  connection_info:
    unknown_key: should_fail
)");
  ParameterContext ctx;
  try {
      ctx.merge_yaml(config);
      assert(false && "Should throw on unknown key in connection_info");
  } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("Unknown configuration key in tdengine_connection") != std::string::npos);
      std::cout << "Unknown key detection test passed.\n";
  }
}

void test_missing_required_key_detection() {
  YAML::Node config = YAML::Load(R"(
global:
  database_info:
    drop_if_exists: true
    precision: ms
)");
  ParameterContext ctx;
  try {
      ctx.merge_yaml(config);
      assert(false && "Should throw on missing required field 'name' in database_info");
  } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("Missing required field 'name' in database_info") != std::string::npos);
      std::cout << "Missing required key detection test passed.\n";
  }
}

void test_nested_unknown_key_detection() {
  YAML::Node config = YAML::Load(R"(
global:
  database_info:
    name: testdb
    drop_if_exists: true
    precision: ms
    props: vgroups 20
    unknown_nested: fail
)");
  ParameterContext ctx;
  try {
      ctx.merge_yaml(config);
      assert(false && "Should throw on unknown key in database_info");
  } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("Unknown configuration key in database_info") != std::string::npos);
      std::cout << "Nested unknown key detection test passed.\n";
  }
}

int main() {
    test_commandline_merge();
    test_environment_merge();
    test_yaml_merge();
    test_priority();
    test_unknown_key_detection();
    test_missing_required_key_detection();
    test_nested_unknown_key_detection();

    std::cout << "All tests passed!\n";
    return 0;
}