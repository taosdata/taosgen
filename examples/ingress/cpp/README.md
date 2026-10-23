# taosgen ingress C++ client example

该目录提供 taosgen ingress gRPC 服务的 C++ 客户端样例，用于指导外部应用如何把解析后的数据推送给 taosgen，由 taosgen 通过 STMT2 写入 TDengine。

样例覆盖两种典型写入组织方式：

| 模式 | 默认数据规模 | proto 数据结构 | 适用场景 |
| --- | ---: | --- | --- |
| `interlace1` | `1000000` 张子表，每表 `100` 行，总计 `100000000` 行 | `InterlaceBatch` | `interlace=1`，每次请求多张表、每表少量行 |
| `interlace0` | `10000` 张子表，每表 `10000` 行，总计 `100000000` 行 | `TableBatch` | `interlace=0`，每次请求一张表的一段连续行 |

## 1. 编译客户端

在本目录执行：

```bash
./build.sh
```

默认生成 Release 版本：

```text
build-release/taosgen_ingress_cpp_client
```

可通过环境变量覆盖构建参数：

```bash
BUILD_TYPE=Debug BUILD_DIR=build-debug JOBS=20 ./build.sh
```

`build.sh` 会优先使用仓库根目录的 Conan 配置安装 gRPC 和 Protobuf 依赖，然后用 CMake 编译客户端。客户端 CMake 工程会直接读取仓库内的 `proto/taosgen/ingress/ingestion.proto`，生成 C++ gRPC stub。

## 2. 启动 interlace=1 服务端

在 taosgen 源码根目录启动服务端：

```bash
./build-release/src/taosgen -c examples/ingress/cpp/config/taosgen-ingress-interlace1.yaml
```

如果从本目录执行，命令如下：

```bash
../../../build-release/src/taosgen -c config/taosgen-ingress-interlace1.yaml
```

该配置会：

1. 创建 `tsbench` 数据库。
2. 创建超级表 `meters`。
3. 创建 `1000000` 张子表，表名为 `d0` 到 `d999999`。
4. 启动监听 `127.0.0.1:7600` 的 ingress gRPC 服务。

## 3. 运行 interlace=1 客户端

```bash
./build-release/taosgen_ingress_cpp_client --mode interlace1
```

默认请求组织：

```text
total_tables = 1000000
rows_per_table = 100
tables_per_request = 10000
rows_per_table_per_request = 1
total_requests = 10000
```

每个请求通过 `InterlaceBatch` 携带 `10000` 张连续子表，每张子表 `1` 行。表名通过 `TableNameRange(prefix="d", start=N, count=10000, step=1)` 表达，避免在每个请求中重复传输大量表名。

## 4. 启动 interlace=0 服务端

停止上一轮 taosgen 服务后，从 taosgen 根目录执行：

```bash
./build-release/src/taosgen -c examples/ingress/cpp/config/taosgen-ingress-interlace0.yaml
```

该配置会创建 `10000` 张子表，每张子表目标写入 `10000` 行。

## 5. 运行 interlace=0 客户端

```bash
./examples/ingress/cpp/build-release/taosgen_ingress_cpp_client --mode interlace0
```

默认请求组织：

```text
total_tables = 10000
rows_per_table = 10000
rows_per_request = 10000
total_requests = 10000
```

每个请求通过 `TableBatch` 携带 `1` 张子表的 `10000` 行数据。该组织方式贴近 `interlace=0` 的顺序写入场景：一个请求写完一张子表的一段连续行。

## 6. 常用参数

```bash
./build-release/taosgen_ingress_cpp_client \
  --mode interlace1 \
  --address 127.0.0.1:7600 \
  --concurrency 10 \
  --ack-mode enqueue_only \
  --deadline-ms 30000
```

参数说明：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--mode` | `interlace1` | `interlace1` 或 `interlace0` |
| `--address` | `127.0.0.1:7600` | taosgen ingress gRPC 地址 |
| `--concurrency` | `10` | 客户端并发线程数 |
| `--ack-mode` | `enqueue_only` | 请求携带的 ack mode；当前 taosgen 性能优先实现中，非 `accept_only` 模式均快速入队返回 |
| `--deadline-ms` | `30000` | 单次 gRPC 调用 deadline |
| `--session-id` | `taosgen-ingress-cpp-example` | 写入 taosgen 日志的全局 session id |
| `--skip-write-wait` | 关闭 | 客户端提交完成后不等待服务端 `written_rows` 追平 |
| `--total-tables` | 模式相关 | 子表数 |
| `--rows-per-table` | 模式相关 | 每张子表总行数 |
| `--tables-per-request` | 模式相关 | 每个请求携带的子表数量 |
| `--rows-per-table-per-request` | 模式相关 | `interlace1` 中每个请求里每张表的行数 |
| `--rows-per-request` | 模式相关 | `interlace0` 中每个 `TableBatch` 的行数 |

## 7. 校验写入结果

客户端结束后会输出本次服务端 stats 增量：

```text
Server Stats Delta: accepted_rows=100000000, written_rows=100000000, failed_rows=0
```

也可以通过 TDengine CLI 校验：

```bash
taos -s "select count(*) from tsbench.meters;"
```

预期结果：

```text
count(*) = 100000000
```

## 8. 外部应用接入要点

1. taosgen 配置文件中的 `schema` 是固定 schema，客户端只传值，不在请求中传任意 schema。
2. `auto_create_table=false` 是预建表最高性能路径，客户端请求不携带 tags。
3. `interlace1` 宽批次优先使用 `InterlaceBatch`，连续表名优先使用 `TableNameRange`。
4. `interlace0` 单表连续行写入可使用 `TableBatch`。
5. `ack_mode` 不用于同步等待 TDengine 写入结果，写入失败通过 taosgen 日志和 `GetStats` 事后分析。
