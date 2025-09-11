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

    const auto& conn_info = ctx.get_tdengine();
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

    const auto& conn_info = ctx.get_tdengine();
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
  from_csv:
    tags:
      file_path: /root/meta/cnnc_csv_1s.csv
      has_header: true
    columns:
      file_path: /root/data/cnnc_csv_1s/
      has_header: true
      timestamp_index: 0
  tbname:
    prefix: s
    count: 10000
    from: 200
  columns: &columns_info
    - name: ts
      type: timestamp
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
    concurrency: 8

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
        uses: tdengine/create-child-table
        with:
          batch:
            size: 1000
            concurrency: 10

  insert-second-data:
    name: Insert Second-Level Data
    needs: [create-second-child-table]
    steps:
      - name: Insert Second-Level Data
        uses: tdengine/insert-data
        with:
          target: tdengine
          format: sql
          concurrency: 8
          time_interval:
            enabled: true
            interval_strategy: first_to_first
)");

    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();

    // Validate global config
    assert(data.global.tdengine.host == "10.0.0.1");
    assert(data.global.tdengine.port == 6043);
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
    assert(data.jobs[2].steps[0].uses == "tdengine/create-child-table");
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
    assert(data.jobs[3].steps[0].uses == "tdengine/insert-data");
    assert(std::holds_alternative<InsertDataConfig>(data.jobs[3].steps[0].action_config));
    const auto& insert_config = std::get<InsertDataConfig>(data.jobs[3].steps[0].action_config);

    assert(insert_config.schema.tbname.source_type == "generator");
    assert(insert_config.schema.tbname.generator.prefix == "s");
    assert(insert_config.schema.tbname.generator.count == 10000);
    assert(insert_config.schema.tbname.generator.from == 200);

    assert(insert_config.schema.columns_cfg.source_type == "csv");
    assert(insert_config.schema.columns_cfg.csv.file_path == "/root/data/cnnc_csv_1s/");
    assert(insert_config.schema.columns_cfg.csv.has_header == true);

    assert(insert_config.schema.columns_cfg.csv.timestamp_strategy.strategy_type == "csv");
    const auto& ts_csv_cfg = insert_config.schema.columns_cfg.csv.timestamp_strategy.csv;
    (void)ts_csv_cfg;
    assert(ts_csv_cfg.timestamp_index == 0);
    assert(ts_csv_cfg.timestamp_precision == "ms");

    assert(insert_config.target_type == "tdengine");
    assert(insert_config.tdengine.protocol_type == TDengineConfig::ProtocolType::Native);
    assert(insert_config.tdengine.host == "10.0.0.1");
    assert(insert_config.tdengine.port == 6043);
    assert(insert_config.tdengine.user == "root");
    assert(insert_config.tdengine.password == "secret");
    assert(insert_config.tdengine.database == "testdb");
    assert(insert_config.schema.name == "points");
    assert(insert_config.schema.columns_cfg.get_schema().size() > 0);
    assert(insert_config.schema.tags_cfg.get_schema().size() > 0);
    assert(insert_config.data_format.format_type == "sql");

    assert(insert_config.schema.generation.interlace_mode.enabled == true);
    assert(insert_config.schema.generation.interlace_mode.rows  == 60);
    assert(insert_config.schema.generation.generate_threads == 8);
    assert(insert_config.schema.generation.per_table_rows == 10000);

    assert(insert_config.schema.generation.per_batch_rows == 10000);
    assert(insert_config.insert_threads == 8);
    assert(insert_config.thread_allocation == "index_range");

    assert(insert_config.time_interval.enabled == true);
    assert(insert_config.time_interval.interval_strategy == "first_to_first");

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
    assert(ctx.get_tdengine().host == "cli.host");
    std::cout << "Priority test passed.\n";
}

void test_unknown_key_detection() {
  YAML::Node config = YAML::Load(R"(
tdengine:
  unknown_key: should_fail
)");
  ParameterContext ctx;
  try {
      ctx.merge_yaml(config);
      assert(false && "Should throw on unknown key in tdengine");
  } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("Unknown configuration key in tdengine") != std::string::npos);
      std::cout << "Unknown key detection test passed.\n";
  }
}

// void test_missing_required_key_detection() {
//   YAML::Node config = YAML::Load(R"(
// schema:
//   tbname:
//     prefix: s
//     count: 1000
//     from: 200
//   tags:
//   columns:
// )");
//   ParameterContext ctx;
//   try {
//       ctx.merge_yaml(config);
//       assert(false && "Should throw on missing required field 'name' in schema");
//   } catch (const std::runtime_error& e) {
//       std::string msg = e.what();
//       assert(msg.find("Missing required field 'name' in schema") != std::string::npos);
//       std::cout << "Missing required key detection test passed.\n";
//   }
// }

void test_nested_unknown_key_detection() {
  YAML::Node config = YAML::Load(R"(
schema:
  name: meters
  tbname:
    prefix: s
    count: 1000
    from: 200
  tags:
  columns:
    - name: ts
      type: timestamp
      unknown_nested: fail
)");
  ParameterContext ctx;
  try {
      ctx.merge_yaml(config);
      assert(false && "Should throw on unknown key in schema::columns");
  } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      assert(msg.find("Unknown configuration key in columns or tags: unknown_nested") != std::string::npos);
      std::cout << "Nested unknown key detection test passed.\n";
  }
}

void test_load_default_schema() {
  ParameterContext ctx;
  YAML::Node config = YAML::Load(R"()");

  ctx.merge_yaml(config);


    // tbname
    const auto& schema = ctx.get_global_config().schema;
    assert(schema.name == "meters");
    assert(schema.tbname.generator.prefix == "d");
    assert(schema.tbname.generator.count == 10000);
    assert(schema.tbname.generator.from == 0);

    // columns
    assert(schema.columns.size() == 4);
    assert(schema.columns[0].name == "ts");
    assert(schema.columns[0].type == "timestamp");
    assert(schema.columns[1].name == "current");
    assert(schema.columns[1].type == "float");
    assert(schema.columns[1].min.value() == 0);
    assert(schema.columns[1].max.value() == 100);
    assert(schema.columns[2].name == "voltage");
    assert(schema.columns[2].type == "int");
    assert(schema.columns[2].min.value() == 200);
    assert(schema.columns[2].max.value() == 240);
    assert(schema.columns[3].name == "phase");
    assert(schema.columns[3].type == "float");
    assert(schema.columns[3].formula.value() == "_i * math.pi % 180");

    // tags
    assert(schema.tags.size() == 2);
    assert(schema.tags[0].name == "groupid");
    assert(schema.tags[0].type == "int");
    assert(schema.tags[0].min.value() == 1);
    assert(schema.tags[0].max.value() == 10);
    assert(schema.tags[1].name == "location");
    assert(schema.tags[1].type == "binary(24)");
    assert(schema.tags[1].str_values.size() == 10);
    assert(schema.tags[1].str_values[0] == "New York");
    assert(schema.tags[1].str_values[9] == "Austin");

    // generation
    assert(schema.generation.interlace_mode.enabled == false);
    assert(schema.generation.per_table_rows == 10000);
    assert(schema.generation.per_batch_rows == 10000);

    std::cout << "Default schema loaded test passed.\n";
}

int main() {
    test_commandline_merge();
    test_environment_merge();
    test_yaml_merge();
    test_priority();
    test_unknown_key_detection();
    // test_missing_required_key_detection();
    test_nested_unknown_key_detection();
    test_load_default_schema();

    std::cout << "All tests passed!\n";
    return 0;
}