<!-- omit in toc -->
# taosgen

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
## Table of Contents
- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
- [4. Build](#4-build)
- [5. Testing](#5-testing)
  - [5.1 Run Tests](#51-run-tests)
  - [5.2 Add Test Cases](#52-add-test-cases)
- [6. CI/CD](#6-cicd)
- [7. Submitting Issues](#7-submitting-issues)
  - [7.1 Required Information](#71-required-information)
  - [7.2 Additional Information](#72-additional-information)
- [8. Submitting PRs](#8-submitting-prs)
- [9. References](#9-references)
- [10. Appendix](#10-appendix)
- [11. License](#11-license)

## 1. Introduction
`taosgen` is a performance benchmarking tool for time-series data products, supporting data generation and write performance testing. `taosgen` uses "jobs" as the basic unit, which are user-defined sets of operations for specific tasks. Each job contains one or more steps and can be connected to other jobs via dependencies, forming a Directed Acyclic Graph (DAG) execution flow for flexible and efficient task orchestration.

Currently, `taosgen` supports Linux and macOS systems.

## 2. Documentation
- For usage, refer to the [Reference Manual](https://docs.taosdata.com/reference/tools/taosgen/), which covers running, command-line arguments, configuration parameters, and sample configuration files.
- This quick guide is mainly for developers who want to contribute, build, and test the `taosgen` tool. For more information about TDengine, visit the [official documentation](https://docs.taosdata.com/).

## 3. Prerequisites
First, ensure TDengine is deployed locally. For detailed deployment steps, see [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Make sure both taosd and taosAdapter services are running.

Before installing and using `taosgen`, ensure you meet the following platform-specific prerequisites:

- cmake, version 3.19 or above. See [cmake](https://cmake.org).
- conan, version 2.19 or above. See [conan](https://conan.io).

## 4. Build
This section provides detailed instructions for building `taosgen` on Linux or macOS platforms.
Before proceeding, make sure you are in the project root directory.

>**Note: This project is developed and compiled using the C++17 standard. Please ensure your compiler supports C++17.**

```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

On macOS, if your compiler does not automatically select the appropriate default SDK, specify CMAKE_OSX_SYSROOT during configuration:
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

## 5. Testing

### 5.1 Run Tests
`taosgen` uses ctest as its test framework. Run `ctest` in the build directory to execute all test cases.

### 5.2 Add Test Cases
Test cases are located in the test directories of each submodule.
- To add test cases to an existing test file: name the test functions with the prefix `test_` and call them in the `main` function.
- To add a new test file: write test cases and a `main` function in the file, and add the build configuration in the corresponding `CMakeLists.txt` in the same directory.

## 6. CI/CD
- [Build Workflow](https://github.com/taosdata/tsgen/actions/workflows/build.yml)
- [Code Coverage] -TODO

## 7. Submitting Issues
We welcome [GitHub Issues](https://github.com/taosdata/taosgen/issues/new?template=Blank+issue). Please provide the following information to help us diagnose and resolve issues efficiently:

### 7.1 Required Information
- Problem Description:
  Provide a clear and detailed description of the issue.
  Indicate whether the issue is persistent or intermittent.
  If possible, include detailed stack traces or error messages to aid diagnosis.

- taosgen version or Commit ID
- taosgen configuration parameters
- TDengine server version

### 7.2 Additional Information
- Operating System: Specify the OS and its version.
- Steps to Reproduce: Provide instructions to reproduce the issue.
- Environment Configuration: Include any relevant environment settings.
- Logs: Attach any logs that may help diagnose the issue.

## 8. Submitting PRs
We welcome contributions! Please follow these steps when submitting a PR:
1. Fork the project ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from `main` with a meaningful name (`git checkout -b my_branch`). Do not modify the `main` branch directly.
3. Make your changes, ensure all unit tests pass, and add new tests to verify your changes.
4. Push your changes to your remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find it under [Pull Requests](https://github.com/taosdata/taosgen/pulls). Click the link to view CI status. If it passes, you'll see “All checks have passed”. You can always click “Show all checks” -> “Details” for detailed logs.
7. After CI passes, you can check your PR's test coverage on [codecov](https://app.codecov.io/gh/taosdata/taosgen/pulls).

## 9. References
- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. Appendix
Project source code layout (directories only):
```
<root>
├── cmake
├── inc
├── src
│   ├── actions
│   │   ├── base
│   │   ├── components
│   │   │   ├── connector
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   ├── formatter
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   ├── garbage_collector
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   ├── generator
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   ├── memory_pool
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   ├── metrics
│   │   │   │   ├── inc
│   │   │   │   ├── src
│   │   │   │   └── test
│   │   │   └── reader
│   │   │       ├── csv
│   │   │       │   ├── inc
│   │   │       │   ├── src
│   │   │       │   └── test
│   │   │       └── kafka
│   │   ├── config
│   │   │   ├── inc
│   │   │   ├── src
│   │   │   └── test
│   │   └── core
│   │       ├── create
│   │       │   ├── inc
│   │       │   ├── src
│   │       │   └── test
│   │       ├── insert
│   │       │   ├── inc
│   │       │   ├── src
│   │       │   │   ├── generator
│   │       │   │   │   ├── inc
│   │       │   │   │   ├── src
│   │       │   │   │   └── test
│   │       │   │   ├── pipeline
│   │       │   │   │   ├── inc
│   │       │   │   │   ├── src
│   │       │   │   │   └── test
│   │       │   │   └── writer
│   │       │   │       ├── inc
│   │       │   │       ├── src
│   │       │   │       └── test
│   │       │   └── test
│   │       ├── query
│   │       │   ├── inc
│   │       │   ├── src
│   │       │   └── test
│   │       └── subscribe
│   │           ├── inc
│   │           ├── src
│   │           └── test
│   ├── engine
│   │   ├── inc
│   │   ├── src
│   │   └── test
│   ├── parameter
│   │   ├── conf
│   │   ├── inc
│   │   ├── src
│   │   └── test
│   ├── utils
│   │   ├── inc
│   │   ├── src
│   │   └── test
│   └── workflow
│       ├── inc
│       ├── src
│       └── test
└── test
```

## 11. License
[MIT License](./LICENSE)