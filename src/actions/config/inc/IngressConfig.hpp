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

#include <cstddef>
#include <algorithm>
#include <string>
#include <thread>

constexpr size_t kIngressMaxRowsPerBatch = 100000000;
constexpr size_t kIngressMaxQueueCapacity = 10000;
constexpr size_t kIngressMaxConcurrency = 1000;

enum class AckMode {
    AcceptOnly,
    EnqueueOnly,
};

inline std::string ack_mode_to_string(AckMode mode) {
    switch (mode) {
        case AckMode::AcceptOnly:
            return "accept_only";
        case AckMode::EnqueueOnly:
            return "enqueue_only";
    }
    return "enqueue_only";
}

struct IngressConfig {
    bool enabled = false;
    std::string listen = "127.0.0.1:7600";
    size_t service_concurrency = std::max<size_t>(std::thread::hardware_concurrency(), 1);
    size_t max_message_bytes = 4 * 1024 * 1024;
    size_t rows_per_batch = 10000;
    size_t max_tables_per_request = 1000;
    AckMode default_ack_mode = AckMode::EnqueueOnly;
    int request_timeout_ms = 30000;
    size_t queue_capacity = 10;
    size_t concurrency = 8;
    bool dry_run = false;
    bool auto_create_table = false;

    struct Writer {
        struct FailureHandling {
            size_t max_retries = 0;
            int retry_interval_ms = 1000;
            std::string on_failure = "warn_and_continue";
        } failure_handling;
    } writer;
};
