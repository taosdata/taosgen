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
#include "TimeIntervalStrategy.hpp"
#include <fstream>
#include "FilesystemCompat.hpp"

class FileSystemWriter : public ISinkPlugin {
public:
    FileSystemWriter(const InsertDataConfig::Target& target);
    ~FileSystemWriter();

    bool connect() override;
    void write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_time_interval_config(
        const InsertDataConfig::TimeInterval& config) override;

    std::string get_timestamp_precision() const override;

private:
    // Get current file path
    fs::path get_current_file_path(int64_t timestamp);

    // Write content
    void write(const std::string& data, int64_t timestamp);

    // Time interval strategy executor
    TimeIntervalStrategy time_strategy_;

    // File output stream
    std::ofstream output_file_;

    // Configuration info
    InsertDataConfig::Target target_config_;

    // Current file info
    std::string current_file_path_;
    int64_t current_file_end_time_ = 0;
};