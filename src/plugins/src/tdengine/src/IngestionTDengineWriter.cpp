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

#include "IngestionTDengineWriter.hpp"
#include "ConnectorFactory.hpp"
#include "LogUtils.hpp"
#include "StmtV2InsertData.hpp"
#include "TDengineConfig.hpp"
#include <chrono>
#include <sstream>
#include <thread>
#include <vector>

namespace {

ColumnConfigVector data_columns(const SchemaConfig& schema) {
    if (schema.columns.size() <= 1) {
        return {};
    }
    return ColumnConfigVector(schema.columns.begin() + 1, schema.columns.end());
}

SchemaConfig prepare_ingress_schema(SchemaConfig schema) {
    schema.apply();
    return schema;
}

size_t effective_writer_count(const IngressConfig& ingress) {
    return std::max<size_t>(ingress.concurrency, 1);
}

std::string make_stmt_sql(const SchemaConfig& schema,
                          const ColumnConfigInstanceVector& col_instances,
                          const ColumnConfigInstanceVector& tag_instances,
                          const TDengineConfig& tdengine,
                          bool auto_create_table) {
    std::ostringstream result;

    if (auto_create_table) {
        result << "INSERT INTO ? USING `" << schema.name << "` TAGS (";
        for (size_t i = 0; i < tag_instances.size(); ++i) {
            result << "?";
            if (i + 1 < tag_instances.size()) {
                result << ",";
            }
        }
        result << ") VALUES(?";
        for (size_t i = 0; i < col_instances.size(); ++i) {
            result << ",?";
        }
        result << ")";
        return result.str();
    }

    if (tdengine.protocol_type == TDengineConfig::ProtocolType::WebSocket) {
        result << "INSERT INTO `" << schema.name << "`(tbname,ts";
        for (const auto& col : col_instances) {
            result << ",`" << col.name() << "`";
        }
        result << ") VALUES(?,?";
        for (size_t i = 0; i < col_instances.size(); ++i) {
            result << ",?";
        }
        result << ")";
        return result.str();
    }

    result << "INSERT INTO ? VALUES(?";
    for (size_t i = 0; i < col_instances.size(); ++i) {
        result << ",?";
    }
    result << ")";
    return result.str();
}

class TDengineIngestionWriter final : public IIngestionWriter {
public:
    TDengineIngestionWriter(const GlobalConfig& global,
                            const IngressConfig& ingress,
                            IngestionConnectorFactory connector_factory)
        : schema_(prepare_ingress_schema(global.schema)),
          tdengine_(get_tdengine_config(global)),
          failure_handling_(ingress.writer.failure_handling),
          col_instances_(ColumnConfigInstanceFactory::create(data_columns(schema_))),
          tag_instances_(ingress.auto_create_table
                             ? ColumnConfigInstanceFactory::create(schema_.tags)
                             : ColumnConfigInstanceVector{}),
          stmt_sql_(make_stmt_sql(schema_, col_instances_, tag_instances_, tdengine_, ingress.auto_create_table)) {
        if (!connector_factory) {
            throw std::runtime_error("ingress TDengine connector factory must not be empty");
        }

        const size_t slot_count = effective_writer_count(ingress);
        slots_.reserve(slot_count);
        for (size_t i = 0; i < slot_count; ++i) {
            slots_.push_back(std::make_unique<Slot>(tdengine_, connector_factory));
        }

        if (!slots_.empty()) {
            LogUtils::info("Inserting data into: {}", tdengine_.get_sink_info());
        }
    }

    IngestionWriteResult write(IngestionMemoryBlockPtr block, size_t worker_index) override {
        if (block == nullptr) {
            return IngestionWriteResult{false, 0, "ingress TDengine writer received empty block"};
        }

        const size_t row_count = block->total_rows;
        Slot& slot = slot_for_worker(worker_index);

        try {
            const IngestionWriteResult result = execute_with_retry(slot, block, row_count);
            return result;
        } catch (const std::exception& e) {
            return IngestionWriteResult{false, 0, e.what()};
        }
    }

private:
    struct Slot {
        Slot(const TDengineConfig& tdengine, const IngestionConnectorFactory& connector_factory)
            : connector(connector_factory(tdengine)) {
            if (!connector) {
                throw std::runtime_error("ingress TDengine connector factory returned null");
            }
        }

        std::unique_ptr<DatabaseConnector> connector;
        bool prepared = false;
    };

    static void reset_slot(Slot& slot) {
        if (slot.connector) {
            slot.connector->close();
        }
        slot.prepared = false;
    }

    static TDengineConfig get_tdengine_config(const GlobalConfig& global) {
        const auto* tdengine = get_plugin_config<TDengineConfig>(global.extensions, "tdengine");
        if (tdengine == nullptr) {
            throw std::runtime_error("TDengine configuration not found in PluginExtensions");
        }
        return *tdengine;
    }

    Slot& slot_for_worker(size_t worker_index) {
        const size_t index = worker_index % slots_.size();
        return *slots_[index];
    }

    bool ensure_ready(Slot& slot) {
        if (slot.prepared && slot.connector->is_connected()) {
            return true;
        }
        slot.prepared = false;
        if (!slot.connector->connect()) {
            return false;
        }
        if (!slot.connector->prepare(stmt_sql_)) {
            slot.connector->close();
            return false;
        }
        slot.prepared = true;
        return true;
    }

    IngestionWriteResult execute_with_retry(Slot& slot, const IngestionMemoryBlockPtr& block, size_t row_count) {
        std::string last_error;
        for (size_t attempt = 0; attempt <= failure_handling_.max_retries; ++attempt) {
            try {
                if (ensure_ready(slot)) {
                    StmtV2InsertData stmt_data(block.get(), false, true);
                    if (slot.connector->execute(stmt_data)) {
                        return IngestionWriteResult{true, row_count, "written"};
                    }
                }
                last_error = "ingress TDengine stmt v2 insert operation failed";
                reset_slot(slot);
            } catch (const std::exception& e) {
                last_error = e.what();
                reset_slot(slot);
            } catch (...) {
                last_error = "ingress TDengine writer caught non-standard exception";
                reset_slot(slot);
            }

            if (attempt < failure_handling_.max_retries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(failure_handling_.retry_interval_ms));
            }
        }

        if (failure_handling_.on_failure == "exit") {
            return IngestionWriteResult{false, 0, last_error.empty() ? "ingress TDengine stmt v2 insert operation failed"
                                                                    : last_error,
                                        true};
        }
        return IngestionWriteResult{false,
                                    0,
                                    last_error.empty() ? "ingress TDengine stmt v2 insert operation failed"
                                                       : last_error};
    }

    SchemaConfig schema_;
    TDengineConfig tdengine_;
    IngressConfig::Writer::FailureHandling failure_handling_;
    ColumnConfigInstanceVector col_instances_;
    ColumnConfigInstanceVector tag_instances_;
    std::string stmt_sql_;
    std::vector<std::unique_ptr<Slot>> slots_;
};

} // namespace

std::unique_ptr<IIngestionWriter> create_tdengine_ingestion_writer(const GlobalConfig& global,
                                                                   const IngressConfig& ingress) {
    return create_tdengine_ingestion_writer(
        global,
        ingress,
        [](const TDengineConfig& tdengine) {
            return ConnectorFactory::create(tdengine);
        });
}

std::unique_ptr<IIngestionWriter> create_tdengine_ingestion_writer(
    const GlobalConfig& global,
    const IngressConfig& ingress,
    IngestionConnectorFactory connector_factory) {
    return std::unique_ptr<IIngestionWriter>(
        new TDengineIngestionWriter(global, ingress, std::move(connector_factory)));
}
