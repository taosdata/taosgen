#pragma once

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
#include "SinkPluginFactory.hpp"
#include "TDengineConfig.hpp"
#include "SqlInsertDataFormatter.hpp"
#include "StmtInsertDataFormatter.hpp"
#include "SchemalessInsertDataFormatter.hpp"
#include "DatabaseConnector.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"


class TDengineSinkPlugin : public BaseSinkPlugin {
public:
    TDengineSinkPlugin(const InsertDataConfig& config,
                       const ColumnConfigInstanceVector& col_instances,
                       const ColumnConfigInstanceVector& tag_instances,
                       size_t no = 0,
                       std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    ~TDengineSinkPlugin() override;

    bool connect() override;
    bool connect_with_source(std::optional<ConnectorSource>& conn_source) override;
    void close() noexcept override;
    bool is_connected() const override;

    bool prepare() override;
    FormatResult format(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover = false) const override;
    bool write(const BaseInsertData& data) override;

    void set_connector(std::unique_ptr<DatabaseConnector> connector);

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);
    std::string get_retry_error_context(std::string_view fallback_reason) const override;

    std::unique_ptr<DatabaseConnector> connector_;
    std::unique_ptr<IInsertDataFormatter> formatter_;
    const TDengineConfig* tc_;

    inline static bool registered_ = []() {
        SinkPluginFactory::register_sink_plugin(
            "tdengine",
            [](const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, const ColumnConfigInstanceVector& tag_instances, size_t no, std::shared_ptr<ActionRegisterInfo> action_info) {
                return std::make_unique<TDengineSinkPlugin>(config, col_instances, tag_instances, no, std::move(action_info));
            });
        return true;
    }();
};
