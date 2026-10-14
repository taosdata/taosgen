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

#include "BaseSinkPlugin.hpp"
#include "FilesystemCompat.hpp"

#include <cassert>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

class TestSinkPlugin final : public BaseSinkPlugin {
public:
    TestSinkPlugin(const InsertDataConfig& config,
                   const ColumnConfigInstanceVector& col_instances,
                   const ColumnConfigInstanceVector& tag_instances)
        : BaseSinkPlugin(config, col_instances, tag_instances) {}

    bool connect() override { return true; }
    void close() noexcept override {}
    bool is_connected() const override { return true; }
    FormatResult format(MemoryPool::MemoryBlock*, bool) const override { return std::string(); }
    bool write(const BaseInsertData&) override { return true; }

    template<typename Func>
    bool run_with_retry(Func&& operation,
                        std::string_view operation_name,
                        const BaseInsertData& data) {
        return execute_with_retry(
            std::forward<Func>(operation),
            operation_name,
            data
        );
    }

    void set_error_context(std::string error_context) {
        error_context_ = std::move(error_context);
    }

private:
    std::string get_retry_error_context(std::string_view fallback_reason) const override {
        if (!error_context_.empty()) {
            return error_context_;
        }
        return BaseSinkPlugin::get_retry_error_context(fallback_reason);
    }

    std::string error_context_;
};

std::string read_file(const std::string& path) {
    std::ifstream input(path);
    return std::string(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()
    );
}

size_t count_occurrences(const std::string& text, const std::string& token) {
    size_t count = 0;
    size_t offset = 0;
    while ((offset = text.find(token, offset)) != std::string::npos) {
        ++count;
        offset += token.size();
    }
    return count;
}

struct TestData {
    ColumnConfigInstanceVector col_instances;
    ColumnConfigInstanceVector tag_instances;
    MemoryPool::MemoryBlock block;
    std::string first_table = "d0";
    std::string last_table = "d1";
    BaseInsertData data;

    TestData()
        : data(&block, col_instances, tag_instances) {
        block.tables.resize(2);
        block.tables[0].table_name = first_table.c_str();
        block.tables[1].table_name = last_table.c_str();
        block.used_tables = 2;
        block.total_rows = 2000;
        data.total_rows = block.total_rows;
    }
};

void prepare_log(const std::string& path) {
    if (fs::exists(path)) {
        fs::remove(path);
    }
    LogUtils::init(LogUtils::Level::Debug, path, 1024 * 1024, 1);
}

void finish_log() {
    LogUtils::shutdown();
}

void test_retry_then_recover() {
    const std::string log_path = "testlog/test_retry_then_recover.log";
    prepare_log(log_path);

    InsertDataConfig config;
    config.failure_handling.max_retries = 2;
    config.failure_handling.retry_interval_ms = 0;
    config.failure_handling.on_failure = "skip";
    TestData test_data;
    TestSinkPlugin sink(config, test_data.col_instances, test_data.tag_instances);
    sink.set_error_context("error_code=0x80000001 error=\"connection unavailable\"");

    size_t attempts = 0;
    const bool success = sink.run_with_retry(
        [&attempts] {
            ++attempts;
            return attempts == 3;
        },
        "sql_insert",
        test_data.data
    );

    finish_log();
    const std::string log = read_file(log_path);
    assert(success);
    (void)success;
    assert(attempts == 3);
    assert(count_occurrences(log, "event=retrying") == 2);
    assert(log.find("event=retrying operation=sql_insert attempt=2 retries=1 max_retries=2") != std::string::npos);
    assert(log.find("event=retrying operation=sql_insert attempt=3 retries=2 max_retries=2") != std::string::npos);
    assert(log.find("event=recovered operation=sql_insert attempt=3 retries=2 max_retries=2") != std::string::npos);
    assert(log.find("table_count=2 table_first=\"d0\" table_last=\"d1\" rows=2000") != std::string::npos);
    assert(log.find("error_code=0x80000001 error=\"connection unavailable\"") != std::string::npos);
    assert(log.find("batch_id=") == std::string::npos);
}

void test_retry_exhausted() {
    const std::string log_path = "testlog/test_retry_exhausted.log";
    prepare_log(log_path);

    InsertDataConfig config;
    config.failure_handling.max_retries = 2;
    config.failure_handling.retry_interval_ms = 0;
    config.failure_handling.on_failure = "skip";
    TestData test_data;
    TestSinkPlugin sink(config, test_data.col_instances, test_data.tag_instances);

    size_t attempts = 0;
    const bool success = sink.run_with_retry(
        [&attempts] {
            ++attempts;
            return false;
        },
        "stmt_v2_insert",
        test_data.data
    );

    finish_log();
    const std::string log = read_file(log_path);
    assert(!success);
    (void)success;
    assert(attempts == 3);
    assert(count_occurrences(log, "event=retrying") == 2);
    assert(count_occurrences(log, "event=failed") == 1);
    assert(log.find("event=failed operation=stmt_v2_insert attempt=3 retries=2 max_retries=2") != std::string::npos);
    assert(log.find("reason=\"operation returned false\" on_failure=skip") != std::string::npos);
}

void test_no_retry() {
    const std::string log_path = "testlog/test_no_retry.log";
    prepare_log(log_path);

    InsertDataConfig config;
    config.failure_handling.max_retries = 0;
    config.failure_handling.on_failure = "skip";
    TestData test_data;
    TestSinkPlugin sink(config, test_data.col_instances, test_data.tag_instances);

    size_t attempts = 0;
    const bool success = sink.run_with_retry(
        [&attempts] {
            ++attempts;
            return false;
        },
        "schemaless_insert",
        test_data.data
    );

    finish_log();
    const std::string log = read_file(log_path);
    assert(!success);
    (void)success;
    assert(attempts == 1);
    assert(log.find("event=retrying") == std::string::npos);
    assert(log.find("event=failed operation=schemaless_insert attempt=1 retries=0 max_retries=0") != std::string::npos);
}

void test_exception_stops_retry() {
    const std::string log_path = "testlog/test_exception_stops_retry.log";
    prepare_log(log_path);

    InsertDataConfig config;
    config.failure_handling.max_retries = 2;
    config.failure_handling.on_failure = "skip";
    TestData test_data;
    TestSinkPlugin sink(config, test_data.col_instances, test_data.tag_instances);

    size_t attempts = 0;
    const bool success = sink.run_with_retry(
        [&attempts]() -> bool {
            ++attempts;
            throw std::runtime_error("invalid payload");
        },
        "sql_insert",
        test_data.data
    );

    finish_log();
    const std::string log = read_file(log_path);
    assert(!success);
    (void)success;
    assert(attempts == 1);
    assert(log.find("event=retrying") == std::string::npos);
    assert(log.find("event=failed operation=sql_insert attempt=1 retries=0 max_retries=2") != std::string::npos);
    assert(log.find("reason=\"invalid payload\"") != std::string::npos);
}

}  // namespace

int main() {
    (void)count_occurrences;
    test_retry_then_recover();
    test_retry_exhausted();
    test_no_retry();
    test_exception_stops_retry();
    fs::remove_all("testlog");
    std::cout << "All sink retry logging tests passed." << std::endl;
    return 0;
}
