#pragma once

/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

#include "ISinkPlugin.hpp"
#include "ISinkContext.hpp"
#include "TimeIntervalStrategy.hpp"
#include "TimeRecorder.hpp"
#include "LogUtils.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"
#include <chrono>
#include <functional>
#include <string_view>
#include <thread>

class BaseSinkPlugin : public ISinkPlugin {
public:
    explicit BaseSinkPlugin(const InsertDataConfig& config,
                            const ColumnConfigInstanceVector& col_instances,
                            const ColumnConfigInstanceVector& tag_instances,
                            std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    virtual ~BaseSinkPlugin() = default;
    bool prepare() override {
        return true;
    }

    bool connect_with_source(std::optional<ConnectorSource>& conn_source) override {
        (void)conn_source;
        return connect();
    }

    // Public method implementations
    std::string get_timestamp_precision() const noexcept override { return timestamp_precision_; }
    const ActionMetrics& get_play_metrics() const noexcept override { return play_metrics_; }
    const ActionMetrics& get_write_metrics() const noexcept override { return write_metrics_; }
    std::chrono::steady_clock::time_point start_write_time() const noexcept override {
        return start_write_time_;
    }
    std::chrono::steady_clock::time_point end_write_time() const noexcept override {
        return end_write_time_;
    }

protected:
    // Public member variables
    const InsertDataConfig& config_;
    const ColumnConfigInstanceVector& col_instances_;
    const ColumnConfigInstanceVector& tag_instances_;
    std::unique_ptr<const ISinkContext> context_;
    std::string timestamp_precision_;
    TimeIntervalStrategy time_strategy_;
    std::chrono::steady_clock::time_point start_write_time_;
    std::chrono::steady_clock::time_point end_write_time_;

    // State tracking
    bool first_write_ = true;
    int64_t last_start_time_ = 0;
    int64_t last_end_time_ = 0;

    // Performance statistics
    ActionMetrics play_metrics_;
    ActionMetrics write_metrics_;

    // Public utility methods
    void apply_time_interval_strategy(int64_t current_start, int64_t current_end);
    std::string get_format_description() const;
    void update_write_state(const BaseInsertData& data, bool success);
    void notify(const BaseInsertData& data, bool success);
    std::shared_ptr<ActionRegisterInfo> action_info_;

    std::string get_retry_batch_context(const BaseInsertData& data) const;
    virtual std::string get_retry_error_context(std::string_view fallback_reason) const;
    void log_retrying(std::string_view operation,
                      std::string_view batch_context,
                      size_t attempt,
                      size_t retries,
                      size_t max_retries,
                      std::string_view error_context) const;
    void log_recovered(std::string_view operation,
                       std::string_view batch_context,
                       size_t attempt,
                       size_t retries,
                       size_t max_retries,
                       std::chrono::milliseconds recovery_time) const;
    void log_retry_failed(std::string_view operation,
                          std::string_view batch_context,
                          size_t attempt,
                          size_t retries,
                          size_t max_retries,
                          std::string_view error_context) const;

    template<typename Func>
    bool execute_with_retry(Func&& operation_func,
                            std::string_view operation,
                            const BaseInsertData& data) {
        const size_t max_retries = config_.failure_handling.max_retries;
        size_t current_retry_count = 0;
        std::chrono::steady_clock::time_point first_failure_time;
        std::string batch_context;
        bool batch_context_initialized = false;
        const auto ensure_batch_context = [&]() -> const std::string& {
            if (!batch_context_initialized) {
                batch_context = get_retry_batch_context(data);
                batch_context_initialized = true;
            }
            return batch_context;
        };

        while (true) {
            try {
                bool success = operation_func();
                if (success) {
                    if (current_retry_count > 0) {
                        const auto recovery_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - first_failure_time
                        );
                        log_recovered(
                            operation,
                            ensure_batch_context(),
                            current_retry_count + 1,
                            current_retry_count,
                            max_retries,
                            recovery_time
                        );
                    }
                    return true;
                }
            } catch (const std::exception& e) {
                log_retry_failed(
                    operation,
                    ensure_batch_context(),
                    current_retry_count + 1,
                    current_retry_count,
                    max_retries,
                    // Exceptions may be unrelated to the connector's previous error;
                    // suppress virtual dispatch so stale connector context is not reused.
                    BaseSinkPlugin::get_retry_error_context(e.what())
                );
                break;
            }

            if (current_retry_count >= max_retries) {
                log_retry_failed(
                    operation,
                    ensure_batch_context(),
                    current_retry_count + 1,
                    current_retry_count,
                    max_retries,
                    get_retry_error_context("operation returned false")
                );
                break;
            }

            if (current_retry_count == 0) {
                first_failure_time = std::chrono::steady_clock::now();
            }

            current_retry_count++;
            log_retrying(
                operation,
                ensure_batch_context(),
                current_retry_count + 1,
                current_retry_count,
                max_retries,
                get_retry_error_context("operation returned false")
            );
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.failure_handling.retry_interval_ms)
            );
        }

        // Failure handling strategy
        if (config_.failure_handling.on_failure == "exit") {
            throw std::runtime_error(std::string(operation) + " operation failed");
        }

        return false;
    }

    void update_play_metrics(const BaseInsertData& data);
};
