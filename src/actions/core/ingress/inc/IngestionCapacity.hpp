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

#include "IngressConfig.hpp"
#include <algorithm>
#include <limits>
#include <stdexcept>

inline void validate_ingress_capacity_config(const IngressConfig& config) {
    if (config.rows_per_batch == 0 || config.max_tables_per_request == 0 ||
        config.queue_capacity == 0 || config.concurrency == 0) {
        throw std::runtime_error("ingress capacity values must be positive.");
    }
    if (config.rows_per_batch > kIngressMaxRowsPerBatch) {
        throw std::runtime_error("ingress.rows_per_batch exceeds supported maximum.");
    }
    if (config.max_tables_per_request > config.rows_per_batch) {
        throw std::runtime_error("ingress.max_tables_per_request must not exceed ingress.rows_per_batch.");
    }
    if (config.queue_capacity > kIngressMaxQueueCapacity) {
        throw std::runtime_error("ingress.queue_capacity exceeds supported maximum.");
    }
    if (config.concurrency > kIngressMaxConcurrency) {
        throw std::runtime_error("ingress.concurrency exceeds supported maximum.");
    }
    if (config.queue_capacity > std::numeric_limits<size_t>::max() / config.concurrency) {
        throw std::runtime_error("ingress queue capacity exceeds supported size.");
    }
}

inline size_t effective_ingress_max_tables_per_request(const IngressConfig& config) {
    validate_ingress_capacity_config(config);
    return config.max_tables_per_request;
}

inline size_t effective_ingress_max_rows_per_table(const IngressConfig& config) {
    const size_t max_tables = effective_ingress_max_tables_per_request(config);
    const size_t rows_per_batch = config.rows_per_batch;
    return (rows_per_batch - 1) / max_tables + 1;
}
