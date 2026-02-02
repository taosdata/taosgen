<!-- omit in toc -->
# taosgen

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taosgen/build.yml)](https://github.com/taosdata/taosgen/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taosgen/branch/main/graph/badge.svg)](https://app.codecov.io/github/taosdata/taosgen)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taosgen)
![GitHub License](https://img.shields.io/github/license/taosdata/taosgen)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taosgen?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

<!-- omit in toc -->
## 目录
- [1. 简介](#1-简介)
- [2. 文档](#2-文档)
- [3. 前置条件](#3-前置条件)
- [4. 构建](#4-构建)
- [5. 测试](#5-测试)
  - [5.1 运行测试](#51-运行测试)
  - [5.2 添加用例](#52-添加用例)
- [6. CI/CD](#6-cicd)
- [7. 提交 Issue](#7-提交-issue)
  - [7.1 必要信息](#71-必要信息)
  - [7.2 额外信息](#72-额外信息)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 附录](#10-附录)
  - [10.1 性能测试](#101-性能测试)
- [11. 许可证](#11-许可证)

## 1. 简介
`taosgen` 是时序数据领域产品的性能基准测试工具，支持数据生成、写入性能测试等功能。`taosgen` 以“作业”为基础单元，作业是由用户定义，用于完成特定任务的一组操作集合。每个作业包含一个或多个步骤，并可通过依赖关系与其他作业连接，形成有向无环图（DAG）式的执行流程，实现灵活高效的任务编排。

`taosgen` 目前支持 Linux 和 macOS 系统。

## 2. 文档
- 使用 `taosgen` 工具，请查阅[参考手册](https://docs.taosdata.com/reference/tools/taosgen/)，其中包含运行、命令行参数、配置文件参数、配置文件示例等内容。
- 本快速指南主要面向那些喜欢自己贡献、构建和测试 `taosgen` 工具的开发者。要了解更多关于 TDengine 的信息，您可以访问[官方文档](https://docs.taosdata.com/)。

## 3. 前置条件
首先，确保 TDengine 已本地部署。有关详细的部署步骤，请参阅[部署TDengine](https://docs.tdengine.com/get-started/deploy-from-package/)。确保 taosd 和 taosAdapter 服务均已启动并运行。

在安装和使用 `taosgen` 之前，请确保您已满足特定平台的以下前置条件。

- cmake，3.19 或以上版本，请参阅 [cmake](https://cmake.org)。
- conan，2.19 或以上版本，请参阅 [conan](https://conan.io/)。

## 4. 构建
本节提供了在 Linux 或 macOS 平台构建 `taosgen` 的详细说明。
在继续之前，请确保您位于该项目的根目录中。

>**注意：本项目使用 C++17 标准进行开发和编译。请确保您的编译器支持 C++17。**

```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

在 macOS 平台中，如果您使用的编译器无法自行选择合适的默认 SDK，那么您需要在配置构建时明确指定 CMAKE_OSX_SYSROOT，例如：
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

## 5. 测试

### 5.1 运行测试
`taosgen` 测试框架使用 ctest 来运行测试用例，在构建目录中运行 `ctest` 命令将运行所有测试用例。

### 5.2 添加用例
测试用例位于各子模块的 test 目录中。
- 在现有测试文件中添加测试用例：测试用例函数名称以 `test_` 开头，并在 `main` 函数中调用。
- 新增测试文件：在文件内编写测试用例和 `main` 函数，并在同目录下的 `CMakeLists.txt` 文件中，添加编译控制相关配置。

## 6. CI/CD
- [Build Workflow](https://github.com/taosdata/tsgen/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/github/taosdata/taosgen)

## 7. 提交 Issue
我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taosgen/issues/new?template=Blank+issue) 。提交时，请提供以下信息以帮助我们更高效地诊断和解决问题：

### 7.1 必要信息
- 问题描述：
  提供您遇到的问题的清晰和详细描述。
  指出问题是持续发生还是间歇性发生。
  如果可能，请包括详细的调用栈或错误消息，以帮助诊断问题。

- taosgen 版本或 Commit ID
- taosgen 配置参数
- TDengine 服务器版本

### 7.2 额外信息
- 操作系统：指定操作系统及其版本。
- 重现步骤：提供说明如何重现问题，这有助于我们复现和验证问题。
- 环境配置：包括任何相关的环境配置。
- 日志：附加任何可能有助于诊断问题的相关日志。

## 8. 提交 PR
我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：
1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taosgen/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taosgen/pulls) 页面找到自己 PR，看单测覆盖率。

## 9. 引用
- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 附录
项目源代码布局，仅目录：
```
<root>
├── cmake
├── conf
└── src
    ├── actions
    │   ├── base
    │   ├── components
    │   │   ├── compressor
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── connector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── encoding
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── expression
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── formatter
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── garbage_collector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── generator
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── memory_pool
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── metrics
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   └── reader
    │   │       └── csv
    │   │           ├── inc
    │   │           ├── src
    │   │           └── test
    │   ├── config
    │   │   ├── inc
    │   │   ├── src
    │   │   └── test
    │   └── core
    │       ├── checkpoint
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── create
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── insert
    │       │   ├── inc
    │       │   ├── src
    │       │   │   ├── generator
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   ├── pipeline
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   └── writer
    │       │   │       ├── inc
    │       │   │       ├── src
    │       │   │       └── test
    │       │   └── test
    │       ├── query
    │       │   └── inc
    │       └── subscribe
    │           ├── inc
    │           └── src
    ├── engine
    │   ├── inc
    │   ├── src
    │   └── test
    ├── parameter
    │   ├── conf
    │   ├── inc
    │   ├── src
    │   └── test
    ├── plugins
    │   ├── inc
    │   └── src
    │       ├── kafka
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── mqtt
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       └── tdengine
    │           ├── inc
    │           ├── src
    │           └── test
    ├── utils
    │   ├── inc
    │   ├── src
    │   └── test
    └── workflow
        ├── inc
        └── src
```

### 10.1 性能测试

- 环境（客户端与服务端一致）：

  | 组件 | 规格 |
  |---|---|
  | 操作系统 | Ubuntu 20.04.6 LTS |
  | CPU | Intel Xeon E5-2650 v3 @ 2.30GHz（Haswell-EP），双路 |
  | 核心/线程 | 20C/40T（每路 10C/20T，超线程） |
  | 缓存 | L3 25MB（cache size: 25600 KB） |
  | 内存 | 251 GB |
  | 存储 | 447 GB SSD × 2，1.76 TB SSD |
  | 软件 | TDengine Enterprise 3.3.8.9（默认）；FlashMQ v1.24.0（默认）；Kafka 2.13-4.1.0（默认） |

- 数据模型：100万子表 meters，电流/电压/相位三列；按 interlace=1 交错写入。
- 结果为示范性数据，实际性能受网络/服务器配置/消息大小/并发度影响。
- 单位：K=千条/秒，M=百万条/秒。

| 目标 | 场景 | 基线 | taosgen | 配置摘要 | 提升 |
|---|---|---:|---:|---|---:|
| TDengine | 1亿行、20线程 | 3.168M rps（taosBenchmark） | 3.534M rps | vgroups=32、stmt2、batch=10k | +11.58% |
| MQTT | 200万行、20线程、单条/消息 | — | 15.15K rps | qos=0、records_per_message=1 | — |
| MQTT | 1亿行、20线程、打包500条/消息 | — | 3.127M rps | qos=0、records_per_message=500 | 显著提升 |
| Kafka（单线程） | 1亿行、官方脚本 | 912.70K rps | 968.93K rps | acks=0、batch优化 | +6.16% |
| Kafka（20并发） | 官方脚本 20 进程 | 2.772M rps | 4.577M rps | taosgen 20 线程 | +65.14% |

说明：
- MQTT 在 QoS0 下，打包发送可显著提高吞吐；Broker 配置与消息大小对结果影响极大。
- TDengine 对标 taosBenchmark，taosgen 在等价模型下具备更高吞吐与更低框架开销。
- Kafka 对标官方脚本，单线程与多并发场景下 taosgen 均有优势。

## 11. 许可证
[MIT License](./LICENSE)
