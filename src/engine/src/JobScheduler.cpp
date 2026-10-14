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

#include "JobScheduler.hpp"
#include "SignalManager.hpp"
#include <iostream>


JobScheduler::JobScheduler(const ConfigData& config)
    : JobScheduler(config, std::make_unique<ProductionStepStrategy>(config.global)) {
}

JobScheduler::JobScheduler(const ConfigData& config, std::unique_ptr<StepExecutionStrategy> strategy)
    : config_(config),
      dag_(std::make_unique<JobDAG>(config.jobs)),
      queue_(std::make_shared<ThreadSafeQueue>()),
      remaining_jobs_(config.jobs.size()),
      step_strategy_(std::move(strategy)) {

    if (dag_->has_cycle()) {
        throw std::runtime_error("DAG contains cycles");
    }

    // Initialize queue
    for (auto* node : dag_->get_initial_nodes()) {
        queue_->enqueue(node);
    }
}

bool JobScheduler::run() {
    // Create thread pool
    const size_t concurrency = config_.concurrency;
    std::vector<std::thread> workers;
    for (size_t i = 0; i < concurrency; ++i) {
        workers.emplace_back([this] { worker_loop(); });
    }

    // Wait for all jobs to complete, periodically checking stop flag
    {
        std::unique_lock<std::mutex> lock(done_mutex_);
        while (remaining_jobs_ != 0 && !stop_execution_.load()) {
            done_cv_.wait_for(lock, std::chrono::milliseconds(100));
            if (SignalManager::interrupted()) {
                interrupted_.store(true);
                stop_execution_.store(true);
            }
        }
    }

    // Ensure queue is stopped and wait for threads
    wakeup();
    for (auto& worker : workers) {
        if (worker.joinable()) worker.join();
    }

    return !stop_execution_.load();
}

void JobScheduler::stop() {
    interrupted_.store(true);
    stop_execution_.store(true);
}

void JobScheduler::wakeup() {
    queue_->stop();
    std::unique_lock<std::mutex> lock(done_mutex_);
    done_cv_.notify_all();
}

void JobScheduler::worker_loop() {
    while (true) {
        if (stop_execution_.load()) return;

        DAGNode* node = queue_->dequeue();
        if (!node) return;

        // Execute job steps
        for (const auto& step : node->job.steps) {
            if (stop_execution_.load()) return;

            bool success = step_strategy_->execute(step);
            if (!success) {
                failed_.store(true);
                stop_execution_.store(true);
                LogUtils::error(
                    "Job step execution failed, exiting (job: {}, step: {})",
                    node->job.name,
                    step.name
                );
                queue_->stop();
                {
                    std::unique_lock<std::mutex> lock(done_mutex_);
                    done_cv_.notify_all();
                }
                return;
            }
        }

        // Update successor nodes
        for (auto* successor : node->successors) {
            int prev = successor->in_degree.fetch_sub(1);
            if (prev == 1) { // Now in-degree is 0
                queue_->enqueue(successor);
            }
        }

        int left = remaining_jobs_.fetch_sub(1);
        if (left == 1) {
            std::unique_lock<std::mutex> lock(done_mutex_);
            done_cv_.notify_all();
        }
    }
}