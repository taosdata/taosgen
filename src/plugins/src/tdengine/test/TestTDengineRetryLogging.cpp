/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "TDengineSinkPlugin.hpp"
#include "FilesystemCompat.hpp"
#include "LogUtils.hpp"

#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <utility>

namespace {

class MockConnector final : public DatabaseConnector {
public:
    MockConnector(size_t failures,
                  std::optional<int64_t> error_code,
                  std::string error_message,
                  bool throw_on_failure)
        : failures_(failures),
          error_code_(error_code),
          error_message_(std::move(error_message)),
          throw_on_failure_(throw_on_failure) {}

    bool connect() override { return true; }
    void close() noexcept override {}
    bool select_db(const std::string&) override { return true; }
    bool prepare(const std::string&) override { return true; }
    bool execute(const std::string&) override { return execute_result(); }
    bool execute(const SqlInsertData&) override { return execute_result(); }
    bool execute(const StmtV2InsertData&) override { return execute_result(); }
    bool execute(const SchemalessInsertData&) override { return execute_result(); }
    bool is_connected() const override { return true; }
    bool is_valid() const override { return true; }
    void reset_state() noexcept override { clear_last_error(); }

private:
    bool execute_result() {
        if (attempts_ < failures_) {
            ++attempts_;
            set_last_error(error_code_, error_message_);
            if (throw_on_failure_) {
                throw std::runtime_error("mock execution exception");
            }
            return false;
        }
        ++attempts_;
        clear_last_error();
        return true;
    }

    size_t failures_;
    size_t attempts_ = 0;
    std::optional<int64_t> error_code_;
    std::string error_message_;
    bool throw_on_failure_;
};

InsertDataConfig create_config(size_t max_retries) {
    InsertDataConfig config;
    config.schema.tbname.source_type = "generator";
    config.schema.columns = {ColumnConfig{"value", "INT"}};
    config.schema.name = "meters";
    config.schema.apply();
    config.data_format.format_type = "sql";
    set_format_opt(config.data_format, "sql", SqlFormatOptions{});
    config.failure_handling.max_retries = max_retries;
    config.failure_handling.retry_interval_ms = 0;
    config.failure_handling.on_failure = "skip";

    set_plugin_config(config.extensions, "tdengine", TDengineConfig{});
    auto* tdengine_config = get_plugin_config_mut<TDengineConfig>(config.extensions, "tdengine");
    assert(tdengine_config != nullptr);
    tdengine_config->dsn = "taos://root:taosdata@localhost:6030/test_action";
    tdengine_config->init();
    return config;
}

std::string read_file(const std::string& path) {
    std::ifstream input(path);
    return std::string{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()
    };
}

struct TDengineTestResult {
    bool success;
    std::string log;
};

TDengineTestResult run_tdengine_write(const std::string& log_file,
                                     size_t max_retries,
                                     size_t failures,
                                     std::optional<int64_t> error_code,
                                     std::string error_message,
                                     bool throw_on_failure = false) {
    fs::remove_all("testlog");
    LogUtils::init(LogUtils::Level::Debug, log_file, 1024 * 1024, 1);

    auto config = create_config(max_retries);
    auto col_instances = ColumnConfigInstanceFactory::create(config.schema.columns_cfg.get_schema());
    auto tag_instances = ColumnConfigInstanceFactory::create(config.schema.tags_cfg.get_schema());
    TDengineSinkPlugin plugin(config, col_instances, tag_instances);

    MultiBatch batch;
    batch.table_batches.emplace_back("d0", std::vector<RowData>{{1500000000000, {42}}});
    batch.update_metadata();
    MemoryPool pool(1, 1, 1, col_instances, tag_instances);
    auto* block = pool.convert_to_memory_block(std::move(batch));
    auto result = plugin.format(block);
    assert(std::holds_alternative<InsertFormatResult>(result));
    const auto& data = std::get<InsertFormatResult>(result);

    plugin.set_connector(std::make_unique<MockConnector>(
        failures,
        error_code,
        std::move(error_message),
        throw_on_failure
    ));
    const bool success = plugin.write(*data);
    LogUtils::shutdown();

    return {success, read_file(log_file)};
}

void test_tdengine_retry_recovered() {
    const auto result = run_tdengine_write(
        "testlog/test_tdengine_retry_recovered.log",
        1,
        1,
        0x1234,
        "mock TDengine failure"
    );
    assert(result.success);
    (void)result;
    assert(result.log.find("event=retrying operation=sql_insert attempt=2 retries=1 max_retries=1") != std::string::npos);
    assert(result.log.find("event=recovered operation=sql_insert attempt=2 retries=1 max_retries=1") != std::string::npos);
    assert(result.log.find("error_code=0x00001234 error=\"mock TDengine failure\"") != std::string::npos);
    assert(result.log.find("table_count=1 table=\"d0\" rows=1") != std::string::npos);
}

void test_tdengine_retry_exhausted() {
    const auto result = run_tdengine_write(
        "testlog/test_tdengine_retry_exhausted.log",
        2,
        3,
        0x1234,
        "mock TDengine failure"
    );
    assert(!result.success);
    (void)result;
    assert(result.log.find("event=failed operation=sql_insert attempt=3 retries=2 max_retries=2") != std::string::npos);
    assert(result.log.find("error_code=0x00001234 error=\"mock TDengine failure\"") != std::string::npos);
}

void test_tdengine_error_without_code() {
    const auto result = run_tdengine_write(
        "testlog/test_tdengine_retry_no_code.log",
        1,
        1,
        std::nullopt,
        "mock TDengine failure without code"
    );
    assert(result.success);
    (void)result;
    assert(result.log.find("error=\"mock TDengine failure without code\"") != std::string::npos);
    assert(result.log.find("error_code=") == std::string::npos);
}

void test_tdengine_empty_error_fallback() {
    const auto result = run_tdengine_write(
        "testlog/test_tdengine_retry_fallback.log",
        1,
        1,
        std::nullopt,
        ""
    );
    assert(result.success);
    (void)result;
    assert(result.log.find("reason=\"operation returned false\"") != std::string::npos);
}

void test_tdengine_exception_ignores_stale_connector_error() {
    const auto result = run_tdengine_write(
        "testlog/test_tdengine_retry_exception.log",
        2,
        1,
        0x1234,
        "stale connector failure",
        true
    );
    assert(!result.success);
    (void)result;
    assert(result.log.find("event=failed operation=sql_insert attempt=1 retries=0 max_retries=2") != std::string::npos);
    assert(result.log.find("reason=\"mock execution exception\"") != std::string::npos);
    assert(result.log.find("error_code=") == std::string::npos);
    assert(result.log.find("stale connector failure") == std::string::npos);
}

}  // namespace

int main() {
    test_tdengine_retry_recovered();
    test_tdengine_retry_exhausted();
    test_tdengine_error_without_code();
    test_tdengine_empty_error_fallback();
    test_tdengine_exception_ignores_stale_connector_error();
    fs::remove_all("testlog");
    std::cout << "All TDengine retry logging tests passed." << std::endl;
    return 0;
}
