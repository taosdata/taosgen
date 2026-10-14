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

#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <stdexcept>
#include <chrono>

#include "ConfigData.hpp"
#include "JobDAG.hpp"
#include "ThreadSafeQueue.hpp"
#include "StepExecutionStrategy.hpp"


// Job scheduler class
class JobScheduler {
public:
    // Constructor, initialize scheduler
    explicit JobScheduler(const ConfigData& config);

    // Constructor, allow custom strategy
    explicit JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy);

    // Factory method for testing
    static std::unique_ptr<JobScheduler> create_for_testing(const ConfigData& config) {
        return std::make_unique<JobScheduler>(
            config, std::make_unique<DebugStepStrategy>(config.global)
        );
    }

    // Run scheduler
    bool run();

    void stop();

    bool has_failure() const { return failed_.load(); }
    bool was_interrupted() const { return interrupted_.load(); }

private:
    // Worker thread loop
    void worker_loop();

    // Stop queue and notify waiting threads (called from normal thread context)
    void wakeup();

    const ConfigData& config_;                              // Configuration data
    std::unique_ptr<JobDAG> dag_;                           // Job dependency graph
    std::shared_ptr<ThreadSafeQueue> queue_;                // Thread-safe queue
    std::atomic<int> remaining_jobs_;                       // Remaining jobs count
    std::mutex done_mutex_;
    std::condition_variable done_cv_;
    std::unique_ptr<StepExecutionStrategy> step_strategy_;  // Step execution strategy

    std::atomic<bool> stop_execution_{false};
    std::atomic<bool> interrupted_{false};
    std::atomic<bool> failed_{false};
};