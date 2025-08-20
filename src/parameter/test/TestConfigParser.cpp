#include <iostream>
#include <sstream>
#include <cassert>
#include <yaml-cpp/yaml.h>
#include "ConfigParser.hpp"


void test_ConnectionInfo() {
    std::string yaml = R"(
host: localhost
port: 6030
user: root
password: taosdata
dsn: "http://localhost:6041"
)";
    YAML::Node node = YAML::Load(yaml);
    ConnectionInfo ci = node.as<ConnectionInfo>();
    assert(ci.host == "localhost");
    assert(ci.port == 6041);
    assert(ci.user == "root");
    assert(ci.password == "taosdata");
    assert(ci.dsn.has_value());
}

void test_DatabaseInfo() {
    std::string yaml = R"(
name: testdb
drop_if_exists: true
precision: ms
properties: "replica=2"
)";
    YAML::Node node = YAML::Load(yaml);
    DatabaseInfo db = node.as<DatabaseInfo>();
    assert(db.name == "testdb");
    assert(db.drop_if_exists == true);
    assert(db.precision == "ms");
    assert(db.properties == "replica=2");
}

void test_ColumnConfig_random() {
    std::string yaml = R"(
name: temperature
type: float
primary_key: false
gen_type: random
distribution: normal
min: 10.0
max: 50.0
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "temperature");
    assert(col.type == "float");
    assert(col.gen_type.has_value() && *col.gen_type == "random");
    assert(col.distribution.has_value() && *col.distribution == "normal");
    assert(col.min.has_value() && *col.min == 10.0);
    assert(col.max.has_value() && *col.max == 50.0);
}

void test_ColumnConfig_order() {
    std::string yaml = R"(
name: id
type: int
gen_type: order
min: 1
max: 100
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "id");
    assert(col.type == "int");
    assert(col.gen_type.has_value() && *col.gen_type == "order");
    assert(col.order_min.has_value() && *col.order_min == 1);
    assert(col.order_max.has_value() && *col.order_max == 100);
}

void test_ColumnConfig_expression() {
    std::string yaml = R"(
name: value
type: float
gen_type: expression
formula: "2*sinusoid(period=10,min=0,max=10)+3"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnConfig col = node.as<ColumnConfig>();
    assert(col.name == "value");
    assert(col.type == "float");
    assert(col.gen_type.has_value() && *col.gen_type == "expression");
    assert(col.formula.has_value());
    assert(col.formula == "2*sinusoid(period=10,min=0,max=10)+3");
}

void test_TableNameConfig_generator() {
    std::string yaml = R"(
source_type: generator
generator:
  prefix: tb
  count: 10
  from: 1
)";
    YAML::Node node = YAML::Load(yaml);
    TableNameConfig tnc = node.as<TableNameConfig>();
    assert(tnc.source_type == "generator");
    assert(tnc.generator.prefix == "tb");
    assert(tnc.generator.count == 10);
    assert(tnc.generator.from == 1);
}

void test_TableNameConfig_csv() {
    std::string yaml = R"(
source_type: csv
csv:
  file_path: tables.csv
  has_header: true
  delimiter: ","
  tbname_index: 0
)";
    YAML::Node node = YAML::Load(yaml);
    TableNameConfig tnc = node.as<TableNameConfig>();
    assert(tnc.source_type == "csv");
    assert(tnc.csv.file_path == "tables.csv");
    assert(tnc.csv.has_header == true);
    assert(tnc.csv.delimiter == ",");
    assert(tnc.csv.tbname_index == 0);
}

void test_TagsConfig_generator() {
    std::string yaml = R"(
source_type: generator
generator:
  schema:
    - name: tag1
      type: int
    - name: tag2
      type: binary(20)
)";
    YAML::Node node = YAML::Load(yaml);
    TagsConfig tags = node.as<TagsConfig>();
    assert(tags.source_type == "generator");
    assert(tags.generator.schema.size() == 2);
    assert(tags.generator.schema[0].name == "tag1");
    assert(tags.generator.schema[1].type == "binary(20)");
    assert(tags.generator.schema[1].type_tag == ColumnTypeTag::BINARY);
}

void test_TagsConfig_csv() {
    std::string yaml = R"(
source_type: csv
csv:
  file_path: tags.csv
  has_header: false
  delimiter: ";"
  exclude_indices: "1,2"
)";
    YAML::Node node = YAML::Load(yaml);
    TagsConfig tags = node.as<TagsConfig>();
    assert(tags.source_type == "csv");
    assert(tags.csv.file_path == "tags.csv");
    assert(tags.csv.has_header == false);
    assert(tags.csv.delimiter == ";");
    assert(!tags.csv.exclude_indices.empty());
}

void test_SuperTableInfo() {
    std::string yaml = R"(
name: st
columns:
  - name: c1
    type: int
  - name: c2
    type: float
tags:
  - name: t1
    type: binary(20)
)";
    YAML::Node node = YAML::Load(yaml);
    SuperTableInfo st = node.as<SuperTableInfo>();
    assert(st.name == "st");
    assert(st.columns.size() == 2);
    assert(st.tags.size() == 1);
}

void test_ChildTableInfo() {
    std::string yaml = R"(
table_name:
  source_type: generator
  generator:
    prefix: tb
    count: 2
    from: 1
tags:
  source_type: generator
  generator:
    schema:
      - name: tag1
        type: int
)";
    YAML::Node node = YAML::Load(yaml);
    ChildTableInfo ct = node.as<ChildTableInfo>();
    assert(ct.table_name.source_type == "generator");
    assert(ct.tags.source_type == "generator");
    assert(ct.tags.generator.schema.size() == 1);
}

void test_CreateChildTableConfig_BatchConfig() {
    std::string yaml = R"(
size: 100
concurrency: 4
)";
    YAML::Node node = YAML::Load(yaml);
    CreateChildTableConfig::BatchConfig bc = node.as<CreateChildTableConfig::BatchConfig>();
    (void)bc;
    assert(bc.size == 100);
    assert(bc.concurrency == 4);
}

void test_TimestampGeneratorConfig() {
    std::string yaml = R"(
start_timestamp: "2023-01-01T00:00:00Z"
timestamp_precision: ms
timestamp_step: 10
)";
    YAML::Node node = YAML::Load(yaml);
    TimestampGeneratorConfig tgc = node.as<TimestampGeneratorConfig>();
    assert(std::holds_alternative<std::string>(tgc.start_timestamp));
    assert(std::get<std::string>(tgc.start_timestamp) == "2023-01-01T00:00:00Z");
    assert(tgc.timestamp_precision == "ms");
    assert(tgc.timestamp_step == 10);
}

void test_TimestampOriginalConfig() {
    std::string yaml = R"(
column_index: 0
precision: ms
offset_config:
  offset_type: relative
  value: "+1d"
)";
    YAML::Node node = YAML::Load(yaml);
    TimestampOriginalConfig toc = node.as<TimestampOriginalConfig>();
    assert(toc.timestamp_index == 0);
    assert(toc.timestamp_precision == "ms");
    assert(toc.offset_config.has_value());
    assert(std::holds_alternative<std::string>(toc.offset_config->value));
}

void test_ColumnsConfig_generator() {
    std::string yaml = R"(
source_type: generator
generator:
  schema:
    - name: c1
      type: int
  timestamp_strategy:
    generator:
      start_timestamp: "2023-01-01T00:00:00Z"
      timestamp_precision: ms
      timestamp_step: 1
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnsConfig cc = node.as<ColumnsConfig>();
    assert(cc.source_type == "generator");
    assert(cc.generator.schema.size() == 1);
    assert(std::holds_alternative<std::string>(cc.generator.timestamp_strategy.timestamp_config.start_timestamp));
    assert(std::get<std::string>(cc.generator.timestamp_strategy.timestamp_config.start_timestamp) == "2023-01-01T00:00:00Z");
    assert(cc.generator.timestamp_strategy.timestamp_config.timestamp_precision == "ms");
    assert(cc.generator.timestamp_strategy.timestamp_config.timestamp_step == 1);
}

void test_ColumnsConfig_csv() {
    std::string yaml = R"(
source_type: csv
csv:
  schema:
    - name: c1
      type: int
  file_path: data.csv
  has_header: true
  delimiter: ","
  timestamp_strategy:
    strategy_type: original
    original:
      column_index: 0
      precision: ms
      offset_config:
        offset_type: relative
        value: "+1d"
)";
    YAML::Node node = YAML::Load(yaml);
    ColumnsConfig cc = node.as<ColumnsConfig>();
    assert(cc.source_type == "csv");
    assert(cc.csv.schema.size() == 1);
    assert(cc.csv.schema[0].name == "c1");
    assert(cc.csv.schema[0].type == "int");
    assert(cc.csv.file_path == "data.csv");
    assert(cc.csv.timestamp_strategy.strategy_type == "original");
}

void test_InsertDataConfig_Source() {
    std::string yaml = R"(
table_name:
  source_type: generator
  generator:
    prefix: tb
    count: 1
    from: 1
columns:
  source_type: generator
  generator:
    schema:
      - name: c1
        type: int
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig::Source src = node.as<InsertDataConfig::Source>();
    assert(src.table_name.source_type == "generator");
    assert(src.columns.source_type == "generator");
}

void test_InsertDataConfig_Target_TDengine() {
    std::string yaml = R"(
target_type: tdengine
tdengine:
  connection_info:
    host: localhost
    port: 6030
    user: root
    password: taosdata
  database_info:
    name: testdb
    drop_if_exists: true
    precision: ms
  super_table_info:
    name: st
    columns:
      - name: c1
        type: int
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig::Target tgt = node.as<InsertDataConfig::Target>();
    assert(tgt.target_type == "tdengine");
    assert(tgt.tdengine.connection_info.host == "localhost");
    assert(tgt.tdengine.database_info.name == "testdb");
    assert(tgt.tdengine.super_table_info.name == "st");
}

void test_InsertDataConfig_Target_FileSystem() {
    std::string yaml = R"(
target_type: file_system
file_system:
  output_dir: ./out
  file_prefix: data
  timestamp_format: "%Y-%m-%d"
  timestamp_interval: "1d"
  include_header: true
  tbname_col_alias: device_id
  compression_level: none
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig::Target tgt = node.as<InsertDataConfig::Target>();
    assert(tgt.target_type == "file_system");
    assert(tgt.file_system.output_dir == "./out");
    assert(tgt.file_system.timestamp_format == "%Y-%m-%d");
}

void test_InsertDataConfig_Target_Mqtt() {
    std::string yaml = R"(
target_type: mqtt
mqtt:
  host: mqtt.example.com
  port: 1883
  user: testuser
  password: testpass
  topic: test/topic
  client_id: client-001
  compression: none
  encoding: utf8
  timestamp_precision: ms
  qos: 1
  keep_alive: 60
  clean_session: true
  retain: false
)";
    YAML::Node node = YAML::Load(yaml);
    InsertDataConfig::Target tgt = node.as<InsertDataConfig::Target>();
    assert(tgt.target_type == "mqtt");
    assert(tgt.mqtt.host == "mqtt.example.com");
    assert(tgt.mqtt.port == 1883);
    assert(tgt.mqtt.user == "testuser");
    assert(tgt.mqtt.password == "testpass");
    assert(tgt.mqtt.client_id == "client-001");
    assert(tgt.mqtt.topic == "test/topic");
    assert(tgt.mqtt.compression == "none");
    assert(tgt.mqtt.encoding == "utf8");
    assert(tgt.mqtt.timestamp_precision == "ms");
    assert(tgt.mqtt.qos == 1);
    assert(tgt.mqtt.keep_alive == 60);
    assert(tgt.mqtt.clean_session == true);
    assert(tgt.mqtt.retain == false);
}

void test_DataFormat_csv() {
    std::string yaml = R"(
format_type: csv
csv:
  delimiter: ";"
  quote_character: "'"
  escape_character: "\\"
)";
    YAML::Node node = YAML::Load(yaml);
    DataFormat df = node.as<DataFormat>();
    assert(df.format_type == "csv");
    assert(df.csv_config.delimiter == ";");
    assert(df.csv_config.quote_character == "'");
    assert(df.csv_config.escape_character == "\\");
}

void test_DataChannel() {
    std::string yaml = R"(
channel_type: native
)";
    YAML::Node node = YAML::Load(yaml);
    DataChannel dc = node.as<DataChannel>();
    assert(dc.channel_type == "native");
}

void test_InsertDataConfig_Control_DataQuality() {
  std::string yaml = R"(
data_disorder:
  enabled: true
  intervals:
    - time_start: "2024-01-01T00:00:00Z"
      time_end: "2024-01-02T00:00:00Z"
      ratio: 0.2
      latency_range: 100
    - time_start: "2024-01-03T00:00:00Z"
      time_end: "2024-01-04T00:00:00Z"
      ratio: 0.5
      latency_range: 200
)";
  YAML::Node node = YAML::Load(yaml);
  InsertDataConfig::Control::DataQuality dq = node.as<InsertDataConfig::Control::DataQuality>();
  assert(dq.data_disorder.enabled == true);
  assert(dq.data_disorder.intervals.size() == 2);
  assert(std::holds_alternative<std::string>(dq.data_disorder.intervals[0].time_start));
  assert(std::get<std::string>(dq.data_disorder.intervals[0].time_start) == "2024-01-01T00:00:00Z");
  assert(dq.data_disorder.intervals[0].ratio == 0.2);
  assert(dq.data_disorder.intervals[1].latency_range == 200);
}

void test_InsertDataConfig_Control_DataGeneration() {
  std::string yaml = R"(
interlace_mode:
  enabled: true
  rows: 10
data_cache:
  enabled: true
  cache_size: 1000
flow_control:
  enabled: true
  rate_limit: 10000
generate_threads: 8
per_table_rows: 10000
queue_capacity: 128
)";
  YAML::Node node = YAML::Load(yaml);
  InsertDataConfig::Control::DataGeneration dg = node.as<InsertDataConfig::Control::DataGeneration>();
  (void)dg;
  assert(dg.interlace_mode.enabled == true);
  assert(dg.interlace_mode.rows == 10);
  assert(dg.data_cache.enabled == true);
  assert(dg.data_cache.cache_size == 1000);
  assert(dg.flow_control.enabled == true);
  assert(dg.flow_control.rate_limit == 10000);
  assert(dg.generate_threads == 8);
  assert(dg.per_table_rows == 10000);
  assert(dg.queue_capacity == 128);
}

void test_InsertDataConfig_Control_InsertControl() {
  std::string yaml = R"(
per_request_rows: 5000
auto_create_table: true
insert_threads: 4
thread_allocation: vgroup_binding
log_path: "insert.log"
enable_dryrun: true
preload_table_meta: true
failure_handling:
  max_retries: 3
  retry_interval_ms: 500
  on_failure: "continue"
)";
  YAML::Node node = YAML::Load(yaml);
  InsertDataConfig::Control::InsertControl ic = node.as<InsertDataConfig::Control::InsertControl>();
  assert(ic.per_request_rows == 5000);
  assert(ic.auto_create_table == true);
  assert(ic.insert_threads == 4);
  assert(ic.thread_allocation == "vgroup_binding");
  assert(ic.log_path == "insert.log");
  assert(ic.enable_dryrun == true);
  assert(ic.preload_table_meta == true);
  assert(ic.failure_handling.max_retries == 3);
  assert(ic.failure_handling.retry_interval_ms == 500);
  assert(ic.failure_handling.on_failure == "continue");
}

void test_InsertDataConfig_Control_TimeInterval() {
  // fixed_interval
  std::string yaml_fixed = R"(
enabled: true
interval_strategy: fixed
wait_strategy: sleep
fixed_interval:
  base_interval: 1000
  random_deviation: 50
)";
  YAML::Node node_fixed = YAML::Load(yaml_fixed);
  InsertDataConfig::Control::TimeInterval ti_fixed = node_fixed.as<InsertDataConfig::Control::TimeInterval>();
  assert(ti_fixed.enabled == true);
  assert(ti_fixed.interval_strategy == "fixed");
  assert(ti_fixed.fixed_interval.base_interval == 1000);
  assert(ti_fixed.fixed_interval.random_deviation == 50);

  // dynamic_interval
  std::string yaml_dynamic = R"(
enabled: true
interval_strategy: first_to_first
wait_strategy: sleep
dynamic_interval:
  min_interval: 10
  max_interval: 100
)";
  YAML::Node node_dynamic = YAML::Load(yaml_dynamic);
  InsertDataConfig::Control::TimeInterval ti_dynamic = node_dynamic.as<InsertDataConfig::Control::TimeInterval>();
  assert(ti_dynamic.enabled == true);
  assert(ti_dynamic.interval_strategy == "first_to_first");
  assert(ti_dynamic.dynamic_interval.min_interval == 10);
  assert(ti_dynamic.dynamic_interval.max_interval == 100);
}

void test_InsertDataConfig_Control() {
  std::string yaml = R"(
data_format:
  format_type: csv
  csv:
    delimiter: ","
data_channel:
  channel_type: native
data_quality:
  data_disorder:
    enabled: false
data_generation:
  interlace_mode:
    enabled: false
    rows: 1
insert_control:
  per_request_rows: 1000
  auto_create_table: false
  insert_threads: 2
  thread_allocation: index_range
  log_path: "result.txt"
  enable_dryrun: false
  preload_table_meta: false
  failure_handling:
    max_retries: 1
    retry_interval_ms: 100
    on_failure: "exit"
time_interval:
  enabled: true
  interval_strategy: fixed
  wait_strategy: sleep
  fixed_interval:
    base_interval: 100
    random_deviation: 10
)";
  YAML::Node node = YAML::Load(yaml);
  InsertDataConfig::Control ctrl = node.as<InsertDataConfig::Control>();
  assert(ctrl.data_format.format_type == "csv");
  assert(ctrl.data_channel.channel_type == "native");
  assert(ctrl.data_quality.data_disorder.enabled == false);
  assert(ctrl.data_generation.interlace_mode.enabled == false);
  assert(ctrl.insert_control.per_request_rows == 1000);
  assert(ctrl.insert_control.insert_threads == 2);
  assert(ctrl.time_interval.enabled == true);
  assert(ctrl.time_interval.fixed_interval.base_interval == 100);
  assert(ctrl.time_interval.fixed_interval.random_deviation == 10);
}

void test_QueryDataConfig_Source() {
  std::string yaml = R"(
connection_info:
  host: localhost
  port: 6030
  user: root
  password: taosdata
)";
  YAML::Node node = YAML::Load(yaml);
  QueryDataConfig::Source src = node.as<QueryDataConfig::Source>();
  assert(src.connection_info.host == "localhost");
  assert(src.connection_info.port == 6030);
  assert(src.connection_info.user == "root");
  assert(src.connection_info.password == "taosdata");
}

void test_QueryDataConfig_Control_QueryControl_Execution() {
  std::string yaml = R"(
mode: parallel
threads: 4
times: 10
interval: 100
)";
  YAML::Node node = YAML::Load(yaml);
  QueryDataConfig::Control::QueryControl::Execution exec = node.as<QueryDataConfig::Control::QueryControl::Execution>();
  assert(exec.mode == "parallel");
  assert(exec.threads == 4);
  assert(exec.times == 10);
  assert(exec.interval == 100);
}

void test_QueryDataConfig_Control_QueryControl_Fixed() {
  std::string yaml = R"(
queries:
  - sql: "select * from tb1"
    output_file: "out1.txt"
  - sql: "select * from tb2"
    output_file: "out2.txt"
)";
  YAML::Node node = YAML::Load(yaml);
  QueryDataConfig::Control::QueryControl::Fixed fixed = node.as<QueryDataConfig::Control::QueryControl::Fixed>();
  assert(fixed.queries.size() == 2);
  assert(fixed.queries[0].sql == "select * from tb1");
  assert(fixed.queries[0].output_file == "out1.txt");
  assert(fixed.queries[1].sql == "select * from tb2");
  assert(fixed.queries[1].output_file == "out2.txt");
}

void test_QueryDataConfig_Control_QueryControl_SuperTable() {
  std::string yaml = R"(
database_name: testdb
super_table_name: st
placeholder: "{tb}"
templates:
  - sql_template: "select * from {tb} limit 10"
    output_file: "tb1.txt"
  - sql_template: "select count(*) from {tb}"
    output_file: "tb2.txt"
)";
  YAML::Node node = YAML::Load(yaml);
  QueryDataConfig::Control::QueryControl::SuperTable st = node.as<QueryDataConfig::Control::QueryControl::SuperTable>();
  assert(st.database_name == "testdb");
  assert(st.super_table_name == "st");
  assert(st.placeholder == "{tb}");
  assert(st.templates.size() == 2);
  assert(st.templates[0].sql_template == "select * from {tb} limit 10");
  assert(st.templates[0].output_file == "tb1.txt");
  assert(st.templates[1].sql_template == "select count(*) from {tb}");
  assert(st.templates[1].output_file == "tb2.txt");
}

void test_QueryDataConfig_Control_QueryControl() {
  // fixed type
  std::string yaml_fixed = R"(
log_path: "query.log"
enable_dryrun: true
execution:
  mode: parallel
  threads: 2
  times: 5
  interval: 50
query_type: fixed
fixed:
  queries:
    - sql: "select 1"
      output_file: "out.txt"
)";
  YAML::Node node_fixed = YAML::Load(yaml_fixed);
  QueryDataConfig::Control::QueryControl qc_fixed = node_fixed.as<QueryDataConfig::Control::QueryControl>();
  assert(qc_fixed.log_path == "query.log");
  assert(qc_fixed.enable_dryrun == true);
  assert(qc_fixed.execution.mode == "parallel");
  assert(qc_fixed.query_type == "fixed");
  assert(qc_fixed.fixed.queries.size() == 1);
  assert(qc_fixed.fixed.queries[0].sql == "select 1");

  // super_table type
  std::string yaml_st = R"(
log_path: "query.log"
enable_dryrun: false
execution:
  mode: sequential_per_thread
  threads: 1
  times: 1
  interval: 0
query_type: super_table
super_table:
  database_name: testdb
  super_table_name: st
  placeholder: "{tb}"
  templates:
    - sql_template: "select * from {tb}"
      output_file: "tb.txt"
)";
  YAML::Node node_st = YAML::Load(yaml_st);
  QueryDataConfig::Control::QueryControl qc_st = node_st.as<QueryDataConfig::Control::QueryControl>();
  assert(qc_st.query_type == "super_table");
  assert(qc_st.super_table.database_name == "testdb");
  assert(qc_st.super_table.templates.size() == 1);
  assert(qc_st.super_table.templates[0].sql_template == "select * from {tb}");
}

void test_QueryDataConfig_Control() {
  std::string yaml = R"(
data_format:
  format_type: csv
  csv:
    delimiter: ","
data_channel:
  channel_type: native
query_control:
  log_path: "query.log"
  enable_dryrun: false
  execution:
    mode: parallel
    threads: 2
    times: 3
    interval: 10
  query_type: fixed
  fixed:
    queries:
      - sql: "select 1"
        output_file: "out.txt"
)";
  YAML::Node node = YAML::Load(yaml);
  QueryDataConfig::Control ctrl = node.as<QueryDataConfig::Control>();
  assert(ctrl.data_format.format_type == "csv");
  assert(ctrl.data_channel.channel_type == "native");
  assert(ctrl.query_control.log_path == "query.log");
  assert(ctrl.query_control.execution.threads == 2);
  assert(ctrl.query_control.fixed.queries.size() == 1);
  assert(ctrl.query_control.fixed.queries[0].sql == "select 1");
}

void test_SubscribeDataConfig_Source() {
  std::string yaml = R"(
connection_info:
  host: localhost
  port: 6030
  user: root
  password: taosdata
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Source src = node.as<SubscribeDataConfig::Source>();
  assert(src.connection_info.host == "localhost");
  assert(src.connection_info.port == 6030);
  assert(src.connection_info.user == "root");
  assert(src.connection_info.password == "taosdata");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Execution() {
  std::string yaml = R"(
consumer_concurrency: 3
poll_timeout: 2000
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::Execution exec = node.as<SubscribeDataConfig::Control::SubscribeControl::Execution>();
  (void)exec;
  assert(exec.consumer_concurrency == 3);
  assert(exec.poll_timeout == 2000);
}

void test_SubscribeDataConfig_Control_SubscribeControl_Topic() {
  std::string yaml = R"(
name: topic1
sql: "select * from st"
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::Topic topic = node.as<SubscribeDataConfig::Control::SubscribeControl::Topic>();
  assert(topic.name == "topic1");
  assert(topic.sql == "select * from st");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Commit() {
  std::string yaml = R"(
mode: manual
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::Commit commit = node.as<SubscribeDataConfig::Control::SubscribeControl::Commit>();
  assert(commit.mode == "manual");
}

void test_SubscribeDataConfig_Control_SubscribeControl_GroupID() {
  std::string yaml = R"(
strategy: custom
custom_id: "group-123"
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::GroupID gid = node.as<SubscribeDataConfig::Control::SubscribeControl::GroupID>();
  assert(gid.strategy == "custom");
  assert(gid.custom_id == "group-123");
}

void test_SubscribeDataConfig_Control_SubscribeControl_Output() {
  std::string yaml = R"(
path: "./out"
file_prefix: "sub"
expected_rows: 100
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl::Output out = node.as<SubscribeDataConfig::Control::SubscribeControl::Output>();
  assert(out.path == "./out");
  assert(out.file_prefix == "sub");
  assert(out.expected_rows == 100);
}

void test_SubscribeDataConfig_Control_SubscribeControl() {
  std::string yaml = R"(
log_path: "sub.log"
enable_dryrun: true
execution:
  consumer_concurrency: 2
  poll_timeout: 1500
topics:
  - name: topic1
    sql: "select * from st"
  - name: topic2
    sql: "select count(*) from st"
commit:
  mode: auto
group_id:
  strategy: custom
  custom_id: "gid-1"
output:
  path: "./out"
  file_prefix: "sub"
  expected_rows: 100
advanced:
  key1: value1
  key2: value2
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control::SubscribeControl ctrl = node.as<SubscribeDataConfig::Control::SubscribeControl>();
  assert(ctrl.log_path == "sub.log");
  assert(ctrl.enable_dryrun == true);
  assert(ctrl.execution.consumer_concurrency == 2);
  assert(ctrl.topics.size() == 2);
  assert(ctrl.topics[0].name == "topic1");
  assert(ctrl.commit.mode == "auto");
  assert(ctrl.group_id.strategy == "custom");
  assert(ctrl.group_id.custom_id == "gid-1");
  assert(ctrl.output.path == "./out");
  assert(ctrl.advanced["key1"] == "value1");
}

void test_SubscribeDataConfig_Control() {
  std::string yaml = R"(
data_format:
  format_type: csv
  csv:
    delimiter: ","
data_channel:
  channel_type: native
subscribe_control:
  log_path: "sub.log"
  enable_dryrun: false
  execution:
    consumer_concurrency: 1
    poll_timeout: 1000
  topics:
    - name: topic1
      sql: "select * from st"
  commit:
    mode: auto
  group_id:
    strategy: default
  output:
    path: "./out"
    file_prefix: "sub"
    expected_rows: 10
)";
  YAML::Node node = YAML::Load(yaml);
  SubscribeDataConfig::Control ctrl = node.as<SubscribeDataConfig::Control>();
  assert(ctrl.data_format.format_type == "csv");
  assert(ctrl.data_channel.channel_type == "native");
  assert(ctrl.subscribe_control.log_path == "sub.log");
  assert(ctrl.subscribe_control.execution.consumer_concurrency == 1);
  assert(ctrl.subscribe_control.topics.size() == 1);
  assert(ctrl.subscribe_control.topics[0].name == "topic1");
  assert(ctrl.subscribe_control.commit.mode == "auto");
  assert(ctrl.subscribe_control.group_id.strategy == "default");
  assert(ctrl.subscribe_control.output.path == "./out");
  assert(ctrl.subscribe_control.output.file_prefix == "sub");
  assert(ctrl.subscribe_control.output.expected_rows == 10);
}

int main() {
    test_ConnectionInfo();
    test_DataFormat_csv();
    test_DataChannel();
    test_DatabaseInfo();
    test_ColumnConfig_random();
    test_ColumnConfig_order();
    test_ColumnConfig_expression();
    test_TableNameConfig_generator();
    test_TableNameConfig_csv();
    test_TagsConfig_generator();
    test_TagsConfig_csv();
    test_SuperTableInfo();
    test_ChildTableInfo();
    test_CreateChildTableConfig_BatchConfig();
    test_TimestampGeneratorConfig();
    test_TimestampOriginalConfig();
    test_ColumnsConfig_generator();
    test_ColumnsConfig_csv();

    test_InsertDataConfig_Source();
    test_InsertDataConfig_Target_TDengine();
    test_InsertDataConfig_Target_FileSystem();
    test_InsertDataConfig_Target_Mqtt();
    test_InsertDataConfig_Control_DataQuality();
    test_InsertDataConfig_Control_DataGeneration();
    test_InsertDataConfig_Control_InsertControl();
    test_InsertDataConfig_Control_TimeInterval();
    test_InsertDataConfig_Control();

    test_QueryDataConfig_Source();
    test_QueryDataConfig_Control_QueryControl_Execution();
    test_QueryDataConfig_Control_QueryControl_Fixed();
    test_QueryDataConfig_Control_QueryControl_SuperTable();
    test_QueryDataConfig_Control_QueryControl();
    test_QueryDataConfig_Control();

    test_SubscribeDataConfig_Source();
    test_SubscribeDataConfig_Control_SubscribeControl_Execution();
    test_SubscribeDataConfig_Control_SubscribeControl_Topic();
    test_SubscribeDataConfig_Control_SubscribeControl_Commit();
    test_SubscribeDataConfig_Control_SubscribeControl_GroupID();
    test_SubscribeDataConfig_Control_SubscribeControl_Output();
    test_SubscribeDataConfig_Control_SubscribeControl();
    test_SubscribeDataConfig_Control();

    std::cout << "All ConfigParser YAML tests passed!" << std::endl;
    return 0;
}