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

#include "FormatResult.hpp"
#include "InsertDataConfig.hpp"
#include "ActionMetrics.hpp"
#include "ConnectorSource.hpp"
#include <optional>
#include <memory>
#include <variant>
#include <chrono>

class ISinkPlugin {
public:
    virtual ~ISinkPlugin() = default;

    // Connection interface (required)
    virtual bool connect() = 0;
    virtual void close() noexcept = 0;
    virtual bool is_connected() const = 0;

    // Connection with source interface (optional)
    virtual bool connect_with_source(std::optional<ConnectorSource>& conn_source) = 0;

    // Prepare interface (optional)
    virtual bool prepare() = 0;

    // Format interface (required)
    virtual FormatResult format(
        MemoryPool::MemoryBlock* block,
        bool is_checkpoint_recover = false
    ) const = 0;

    // Write interface (required)
    virtual bool write(const BaseInsertData& data) = 0;

    // Get timestamp precision
    virtual std::string get_timestamp_precision() const noexcept = 0;

    // Metrics interface
    virtual const ActionMetrics& get_play_metrics() const noexcept = 0;
    virtual const ActionMetrics& get_write_metrics() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point start_write_time() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point end_write_time() const noexcept = 0;
};