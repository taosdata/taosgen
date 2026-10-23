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

#include "DatabaseConnector.hpp"
#include "IngestionTDengineWriter.hpp"
#include "PluginExtensions.hpp"
#include "StmtV2InsertData.hpp"
#include <cassert>
#include <cstdint>
#include <deque>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

struct ConnectorState {
    std::deque<bool> connect_results;
    std::deque<bool> prepare_results;
    std::deque<bool> execute_results;
    bool throw_nonstandard_on_execute = false;
    size_t connect_calls = 0;
    size_t close_calls = 0;
    size_t prepare_calls = 0;
    size_t execute_calls = 0;
    std::vector<std::string> prepared_sql;
};

bool next_result(std::deque<bool>& results) {
    if (results.empty()) {
        return true;
    }
    const bool result = results.front();
    results.pop_front();
    return result;
}

class FakeConnector final : public DatabaseConnector {
public:
    explicit FakeConnector(std::shared_ptr<ConnectorState> state)
        : state_(std::move(state)) {}

    bool connect() override {
        ++state_->connect_calls;
        const bool result = next_result(state_->connect_results);
        connected_ = result;
        return result;
    }

    void close() noexcept override {
        ++state_->close_calls;
        connected_ = false;
        prepared_ = false;
    }

    bool select_db(const std::string&) override {
        return connected_;
    }

    bool prepare(const std::string& sql) override {
        ++state_->prepare_calls;
        state_->prepared_sql.push_back(sql);
        const bool result = next_result(state_->prepare_results);
        prepared_ = result;
        return result;
    }

    bool execute(const std::string&) override {
        return false;
    }

    bool execute(const SqlInsertData&) override {
        return false;
    }

    bool execute(const StmtV2InsertData&) override {
        ++state_->execute_calls;
        if (state_->throw_nonstandard_on_execute) {
            state_->throw_nonstandard_on_execute = false;
            // Deliberately throw a non-exception value to exercise the writer's
            // catch (...) path without relying on std::exception.
            throw 42;
        }
        return next_result(state_->execute_results);
    }

    bool execute(const SchemalessInsertData&) override {
        return false;
    }

    bool is_connected() const override {
        return connected_;
    }

    bool is_valid() const override {
        return connected_ && prepared_;
    }

    void reset_state() noexcept override {
        prepared_ = false;
    }

private:
    std::shared_ptr<ConnectorState> state_;
    bool connected_ = false;
    bool prepared_ = false;
};

struct BlockFixture {
    std::unique_ptr<MemoryPool> pool;
    MemoryPool::MemoryBlock* block = nullptr;

    IngestionMemoryBlockPtr take_block() {
        auto* result = block;
        block = nullptr;
        return IngestionMemoryBlockPtr(result);
    }
};

GlobalConfig make_global(TDengineConfig::ProtocolType protocol,
                         bool auto_create_table = false) {
    GlobalConfig global;
    global.schema.name = "meters";
    global.schema.columns = {
        ColumnConfig{"ts", "TIMESTAMP"},
        ColumnConfig{"value", "INT"}
    };
    if (auto_create_table) {
        global.schema.tags = {
            ColumnConfig{"location", "VARCHAR(32)"}
        };
    }
    global.schema.apply();

    TDengineConfig tdengine("taos://root:taosdata@127.0.0.1:6030/test_db");
    tdengine.protocol_type = protocol;
    set_plugin_config(global.extensions, "tdengine", std::move(tdengine));
    return global;
}

IngressConfig make_ingress(bool auto_create_table = false) {
    IngressConfig ingress;
    ingress.concurrency = 1;
    ingress.auto_create_table = auto_create_table;
    ingress.writer.failure_handling.max_retries = 0;
    ingress.writer.failure_handling.retry_interval_ms = 0;
    ingress.writer.failure_handling.on_failure = "warn_and_continue";
    return ingress;
}

BlockFixture make_block(const GlobalConfig& global) {
    SchemaConfig schema = global.schema;
    schema.apply();
    const ColumnConfigVector data_columns(schema.columns.begin() + 1, schema.columns.end());
    const auto col_instances = ColumnConfigInstanceFactory::create(data_columns);
    const auto tag_instances = ColumnConfigInstanceFactory::create(schema.tags);
    BlockFixture fixture;
    fixture.pool = std::make_unique<MemoryPool>(1, 1, 1, col_instances, tag_instances);

    MultiBatch batch;
    batch.table_batches.push_back({
        "d1001",
        std::vector<RowData>{{1234567890, RowType{int32_t{42}}}}
    });
    batch.update_metadata();
    fixture.block = fixture.pool->convert_to_memory_block(std::move(batch));
    return fixture;
}

std::unique_ptr<IIngestionWriter> make_writer(
    const GlobalConfig& global,
    const IngressConfig& ingress,
    const std::shared_ptr<ConnectorState>& state) {
    return create_tdengine_ingestion_writer(
        global,
        ingress,
        [state](const TDengineConfig&) {
            return std::make_unique<FakeConnector>(state);
        });
}

void test_success_reuses_prepared_slot() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);
    const auto ingress = make_ingress();
    auto state = std::make_shared<ConnectorState>();
    auto writer = make_writer(global, ingress, state);

    BlockFixture first = make_block(global);
    IngestionWriteResult result = writer->write(first.take_block(), 0);
    assert(result.success);
    assert(result.written_rows == 1);
    assert(state->connect_calls == 1);
    assert(state->prepare_calls == 1);
    assert(state->execute_calls == 1);
    assert(state->prepared_sql.size() == 1);
    assert(state->prepared_sql[0] == "INSERT INTO ? VALUES(?,?)");

    BlockFixture second = make_block(global);
    result = writer->write(second.take_block(), 0);
    assert(result.success);
    assert(state->connect_calls == 1);
    assert(state->prepare_calls == 1);
    assert(state->execute_calls == 2);

    std::cout << "test_success_reuses_prepared_slot passed.\n";
}

void test_execute_failure_reconnects_before_retry() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);
    IngressConfig ingress = make_ingress();
    ingress.writer.failure_handling.max_retries = 1;
    auto state = std::make_shared<ConnectorState>();
    state->execute_results = {false, true};
    auto writer = make_writer(global, ingress, state);

    BlockFixture block = make_block(global);
    IngestionWriteResult result = writer->write(block.take_block(), 0);
    assert(result.success);
    assert(result.written_rows == 1);
    assert(state->connect_calls == 2);
    assert(state->prepare_calls == 2);
    assert(state->execute_calls == 2);
    assert(state->close_calls >= 1);

    std::cout << "test_execute_failure_reconnects_before_retry passed.\n";
}

void test_connect_failure_retries() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);
    IngressConfig ingress = make_ingress();
    ingress.writer.failure_handling.max_retries = 1;
    auto state = std::make_shared<ConnectorState>();
    state->connect_results = {false, true};
    auto writer = make_writer(global, ingress, state);

    BlockFixture block = make_block(global);
    IngestionWriteResult result = writer->write(block.take_block(), 0);
    assert(result.success);
    assert(result.written_rows == 1);
    assert(state->connect_calls == 2);
    assert(state->prepare_calls == 1);
    assert(state->execute_calls == 1);
    assert(state->close_calls >= 1);

    std::cout << "test_connect_failure_retries passed.\n";
}

void test_prepare_failure_retries() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);
    IngressConfig ingress = make_ingress();
    ingress.writer.failure_handling.max_retries = 1;
    auto state = std::make_shared<ConnectorState>();
    state->prepare_results = {false, true};
    auto writer = make_writer(global, ingress, state);

    BlockFixture block = make_block(global);
    IngestionWriteResult result = writer->write(block.take_block(), 0);
    assert(result.success);
    assert(state->connect_calls == 2);
    assert(state->prepare_calls == 2);
    assert(state->execute_calls == 1);
    assert(state->close_calls >= 1);

    std::cout << "test_prepare_failure_retries passed.\n";
}

void test_failure_policy_and_nonstandard_exception() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);

    {
        IngressConfig ingress = make_ingress();
        auto state = std::make_shared<ConnectorState>();
        state->execute_results = {false};
        auto writer = make_writer(global, ingress, state);

        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(!result.success);
        assert(!result.fatal);
        assert(result.written_rows == 0);
    }

    {
        IngressConfig ingress = make_ingress();
        ingress.writer.failure_handling.on_failure = "exit";
        auto state = std::make_shared<ConnectorState>();
        state->execute_results = {false};
        auto writer = make_writer(global, ingress, state);

        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(!result.success);
        assert(result.fatal);
        assert(result.written_rows == 0);
    }

    {
        IngressConfig ingress = make_ingress();
        auto state = std::make_shared<ConnectorState>();
        state->throw_nonstandard_on_execute = true;
        auto writer = make_writer(global, ingress, state);

        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(!result.success);
        assert(!result.fatal);
        assert(result.message.find("non-standard exception") != std::string::npos);
    }

    std::cout << "test_failure_policy_and_nonstandard_exception passed.\n";
}

void test_stmt_sql_variants() {
    {
        const auto global = make_global(TDengineConfig::ProtocolType::Native);
        const auto ingress = make_ingress();
        auto state = std::make_shared<ConnectorState>();
        auto writer = make_writer(global, ingress, state);
        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(result.success);
        assert(state->prepared_sql[0] == "INSERT INTO ? VALUES(?,?)");
    }

    {
        const auto global = make_global(TDengineConfig::ProtocolType::WebSocket);
        const auto ingress = make_ingress();
        auto state = std::make_shared<ConnectorState>();
        auto writer = make_writer(global, ingress, state);
        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(result.success);
        assert(state->prepared_sql[0] == "INSERT INTO `meters`(tbname,ts,`value`) VALUES(?,?,?)");
    }

    {
        const auto global = make_global(TDengineConfig::ProtocolType::Native, true);
        const auto ingress = make_ingress(true);
        auto state = std::make_shared<ConnectorState>();
        auto writer = make_writer(global, ingress, state);
        BlockFixture block = make_block(global);
        IngestionWriteResult result = writer->write(block.take_block(), 0);
        assert(result.success);
        assert(state->prepared_sql[0] == "INSERT INTO ? USING `meters` TAGS (?) VALUES(?,?)");
    }

    std::cout << "test_stmt_sql_variants passed.\n";
}

void test_empty_block_is_rejected() {
    const auto global = make_global(TDengineConfig::ProtocolType::Native);
    const auto ingress = make_ingress();
    auto state = std::make_shared<ConnectorState>();
    auto writer = make_writer(global, ingress, state);

    IngestionWriteResult result = writer->write(IngestionMemoryBlockPtr{}, 0);
    assert(!result.success);
    assert(result.written_rows == 0);
    assert(state->connect_calls == 0);
    assert(state->execute_calls == 0);

    std::cout << "test_empty_block_is_rejected passed.\n";
}

} // namespace

int main() {
    test_success_reuses_prepared_slot();
    test_execute_failure_reconnects_before_retry();
    test_connect_failure_retries();
    test_prepare_failure_retries();
    test_failure_policy_and_nonstandard_exception();
    test_stmt_sql_variants();
    test_empty_block_is_rejected();
    std::cout << "All IngestionTDengineWriter tests passed.\n";
    return 0;
}
