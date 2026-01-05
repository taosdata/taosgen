#include "ParameterContext.hpp"
#include "PluginRegistrar.hpp"
#include "TDengineConfig.hpp"
#include "MqttConfig.hpp"
#include "KafkaConfig.hpp"
#include "LogUtils.hpp"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <stdlib.h>

class ScopedEnvVar {
private:
    std::string name_;
    std::string old_value_;
    bool had_old_value_;

public:
    ScopedEnvVar(const std::string& name, const std::string& new_value)
        : name_(name), had_old_value_(false) {

        const char* old = getenv(name.c_str());
        if (old) {
            old_value_ = old;
            had_old_value_ = true;
        }

#ifdef _WIN32
        _putenv_s(name.c_str(), new_value.c_str());
#else
        setenv(name.c_str(), new_value.c_str(), 1);
#endif
    }

    ~ScopedEnvVar() {
        restore();
    }

    void restore() {
#ifdef _WIN32
        if (had_old_value_) {
            _putenv_s(name_.c_str(), old_value_.c_str());
        } else {
            _putenv((name_ + "=").c_str());
        }
#else
        if (had_old_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
#endif
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;
};

// Test command line parameter parsing
void test_commandline_merge() {
    ParameterContext ctx;
    const char* argv[] = {
        "dummy_program",
        "--host=cli.host",
        "--port=1234",
        "--user=cli_user",
        "--password=cli_pass"
    };
    ctx.init(5, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& tc = get_plugin_config<TDengineConfig>(global.extensions, "tdengine");
    (void)tc;
    assert(tc != nullptr);
    assert(tc->host == "cli.host");
    assert(tc->port == 1234);
    assert(tc->user == "cli_user");
    assert(tc->password == "cli_pass");

    // Verify derived URI/bootstrap_servers are updated
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(mc->uri == "tcp://cli.host:1234");

    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "cli.host:1234");

    (void)global;
    std::cout << "Commandline merge test passed.\n";
}

// Test environment variable merge
void test_environment_merge() {
    ParameterContext ctx;

    ScopedEnvVar host_guard("TAOS_HOST", "env.host");
    ScopedEnvVar port_guard("TAOS_PORT", "5678");

    const char* argv[] = {"dummy"};
    ctx.init(1, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& tc = get_plugin_config<TDengineConfig>(global.extensions, "tdengine");
    (void)tc;
    assert(tc != nullptr);
    assert(tc->host == "env.host");
    assert(tc->port == 5678);

    // Verify derived URI/bootstrap_servers are updated
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    (void)mc;
    assert(mc != nullptr);
    assert(mc->uri == "tcp://env.host:5678");

    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "env.host:5678");

    (void)global;
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
  columns:
    - name: ts
      type: timestamp
    - name: latitude
      type: float
    - name: longitude
      type: float
    - name: quality
      type: varchar(50)
  tags:
    - name: type
      type: varchar(7)
    - name: name
      type: varchar(20)
    - name: department
      type: varchar(7)
  generation:
    interlace: 60
    rows_per_table: 10000
    rows_per_batch: 10000
    concurrency: 8

concurrency: 4
jobs:
  create-database:
    name: Create Database
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
        uses: tdengine/insert
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
    const auto& gtc = get_plugin_config<TDengineConfig>(data.global.extensions, "tdengine");
    (void)gtc;
    assert(gtc != nullptr);
    assert(gtc->host == "10.0.0.1");
    assert(gtc->port == 6043);
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
    assert(data.jobs[3].steps[0].uses == "tdengine/insert");
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

    const auto& tc = get_plugin_config<TDengineConfig>(insert_config.extensions, "tdengine");
    (void)tc;
    assert(tc != nullptr);
    assert(insert_config.target_type == "tdengine");
    assert(tc->protocol_type == TDengineConfig::ProtocolType::Native);
    assert(tc->host == "10.0.0.1");
    assert(tc->port == 6043);
    assert(tc->user == "root");
    assert(tc->password == "secret");
    assert(tc->database == "testdb");
    assert(insert_config.schema.name == "points");
    assert(insert_config.schema.columns_cfg.get_schema().size() > 0);
    assert(insert_config.schema.tags_cfg.get_schema().size() == 0);
    assert(insert_config.data_format.format_type == "sql");

    assert(insert_config.schema.generation.interlace_mode.enabled == true);
    assert(insert_config.schema.generation.interlace_mode.rows  == 60);
    assert(insert_config.schema.generation.generate_threads == 8);
    assert(insert_config.schema.generation.rows_per_table == 10000);

    assert(insert_config.schema.generation.rows_per_batch == 10000);
    assert(insert_config.insert_threads == 8);
    assert(insert_config.thread_allocation == "index_range");

    assert(insert_config.time_interval.enabled == true);
    assert(insert_config.time_interval.interval_strategy == "first_to_first");

    std::cout << "YAML merge test passed.\n";
}

// Test parameter priority (command line > environment variable > YAML)
void test_priority() {
    // 1. Set Environment Variable (middle priority)
    ScopedEnvVar host_guard("TAOS_HOST", "env.host");
    ScopedEnvVar port_guard("TAOS_PORT", "6042");

    // 2. Prepare YAML file (lowest priority)
    const char* config_content = "tdengine:\n  dsn: taos+ws://yaml.host:6041";
    FILE* fp = fopen("priority_test.yaml", "w");
    fputs(config_content, fp);
    fclose(fp);

    // 3. Prepare Command Line Arguments (highest priority)
    const char* argv[] = {
        "dummy",
        "--config-file=priority_test.yaml",
        "--host=cli.host" // Port is not set via CLI
    };

    ParameterContext ctx;
    ctx.init(3, const_cast<char**>(argv));

    const auto& tdengine = ctx.get_tdengine();

    // Host should be from CLI (highest priority)
    assert(tdengine.host == "cli.host");
    // Port should be from Environment (middle priority, as CLI did not set it)
    assert(tdengine.port == 6042);
    (void)tdengine;

    // Clean up
    remove("priority_test.yaml");
    std::cout << "Priority test (CLI > Env > YAML) passed.\n";
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
    const char* argv[] = {"dummy"};
    ParameterContext ctx;
    ctx.init(1, const_cast<char**>(argv));

    // tbname
    const auto& schema = ctx.get_global_config().schema;
    (void)schema;
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
    assert(schema.generation.rows_per_table == 10000);
    assert(schema.generation.rows_per_batch == 10000);

    std::cout << "Default schema loaded test passed.\n";
}

void test_mqtt_kafka_global_config() {
    ParameterContext ctx;
    YAML::Node config = YAML::Load(R"(
mqtt:
  uri: "tcp://mqtt.broker:1883"
kafka:
  bootstrap_servers: "kafka.broker:9092"
)");
    ctx.merge_yaml(config);
    const auto& global = ctx.get_global_config();
    const auto& mc = get_plugin_config<MqttConfig>(global.extensions, "mqtt");
    assert(mc != nullptr);
    assert(mc->uri == "tcp://mqtt.broker:1883");

    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "kafka.broker:9092");

    (void)global;
    (void)mc;
    (void)kc;
    std::cout << "MQTT/Kafka global config test passed.\n";
}

void test_insert_action_inheritance() {
    ParameterContext ctx;
    YAML::Node config = YAML::Load(R"(
tdengine:
  dsn: taos://root:taosdata@global.host/global_db
schema:
  name: global_schema
  columns:
    - name: c1
      type: int
jobs:
  insert-job:
    steps:
      - uses: tdengine/insert
        with:
          schema:
            name: local_schema # Override schema name
)");
    ctx.merge_yaml(config);
    const auto& data = ctx.get_config_data();
    assert(data.jobs.size() == 1);
    const auto& insert_config = std::get<InsertDataConfig>(data.jobs[0].steps[0].action_config);

    // Inherits connection from global
    const auto& tc = get_plugin_config<TDengineConfig>(insert_config.extensions, "tdengine");
    (void)tc;
    assert(tc != nullptr);
    assert(tc->host == "global.host");
    assert(tc->database == "global_db");

    // Inherits columns from global schema, but name is overridden locally
    assert(insert_config.schema.name == "local_schema");
    assert(insert_config.schema.columns.size() == 2);
    assert(insert_config.schema.columns[0].name == "ts");
    assert(insert_config.schema.columns[1].name == "c1");

    (void)insert_config;
    std::cout << "Insert action inheritance test passed.\n";
}

void test_init_with_config_file() {
    const char* config_content = "tdengine:\n  dsn: taos+ws://file.host:6041";
    FILE* fp = fopen("file_test.yaml", "w");
    fputs(config_content, fp);
    fclose(fp);

    ParameterContext ctx;
    const char* argv[] = {"dummy", "--config-file=file_test.yaml"};
    ctx.init(2, const_cast<char**>(argv));

    assert(ctx.get_tdengine().host == "file.host");

    remove("file_test.yaml");
    std::cout << "Init with config file test passed.\n";
}

void test_auto_create_database_step() {
    ParameterContext ctx;
    YAML::Node config = YAML::Load(R"(
jobs:
  create-stb:
    steps:
      - uses: tdengine/create-super-table
)");
    ctx.merge_yaml_global(config);
    ctx.merge_yaml_jobs(config);

    const auto& jobs = ctx.get_config_data().jobs;
    assert(jobs.size() == 1);
    assert(jobs[0].steps.size() == 2); // create-database was added
    assert(jobs[0].steps[0].uses == "tdengine/create-database");
    assert(jobs[0].steps[1].uses == "tdengine/create-super-table");

    (void)jobs;
    std::cout << "Auto create database step test passed.\n";
}

void test_show_version() {
  ParameterContext ctx;

  std::stringstream buffer;
  std::streambuf* old = std::cout.rdbuf(buffer.rdbuf());

  ctx.show_version();

  std::cout.rdbuf(old);

  std::string output = buffer.str();

  // Check the output content
  assert(output.find("taosgen version: ") != std::string::npos);
  assert(output.find("git:") != std::string::npos);
  assert(output.find("build:") != std::string::npos);

  std::cout << "show_version test passed.\n";
}

int main() {
    register_plugin_hooks();
    test_commandline_merge();
    test_environment_merge();
    test_yaml_merge();
    test_priority();
    test_unknown_key_detection();
    test_nested_unknown_key_detection();
    test_load_default_schema();
    test_mqtt_kafka_global_config();
    test_insert_action_inheritance();
    test_init_with_config_file();
    test_auto_create_database_step();
    test_show_version();

    std::cout << "All tests passed!\n";
    return 0;
}