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

#include "SignalManager.hpp"

namespace SignalManager {

static std::atomic<bool> g_interrupted{false};

struct SignalCallbackList {
    std::vector<SignalCallback> normal_callbacks;
    std::optional<SignalCallback> final_callback;
};

static std::map<int, SignalCallbackList> callbacks;
static std::mutex cb_mutex;

bool interrupted() {
    return g_interrupted.load(std::memory_order_relaxed);
}

void signal_handler(int signum) {
    g_interrupted.store(true, std::memory_order_relaxed);
    // Use try_lock to avoid deadlock if signal interrupts code holding cb_mutex.
    // If lock fails, callbacks are skipped — the global interrupt flag is already set,
    // which is sufficient for cooperative shutdown.
    std::unique_lock<std::mutex> lock(cb_mutex, std::try_to_lock);
    if (!lock.owns_lock()) return;
    auto it = callbacks.find(signum);
    if (it != callbacks.end()) {
        for (auto& cb : it->second.normal_callbacks) {
            cb(signum);
        }
        if (it->second.final_callback) {
            it->second.final_callback.value()(signum);
        }
    }
}

void register_signal(int signum) {
    std::lock_guard<std::mutex> lock(cb_mutex);
    callbacks[signum]; // Ensure entry exists so setup() installs the handler
}

void register_signal(int signum, SignalCallback cb, bool is_final) {
    std::lock_guard<std::mutex> lock(cb_mutex);
    if (is_final) {
        callbacks[signum].final_callback = std::move(cb);
    } else {
        callbacks[signum].normal_callbacks.push_back(std::move(cb));
    }
}

void setup() {
#if defined(__unix__) || defined(__APPLE__)
    std::lock_guard<std::mutex> lock(cb_mutex);
    struct sigaction sa {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    for (const auto& kv : callbacks) {
        ::sigaction(kv.first, &sa, nullptr);
    }
#else
    std::lock_guard<std::mutex> lock(cb_mutex);
    for (const auto& kv : callbacks) {
        std::signal(kv.first, signal_handler);
    }
#endif
}

} // namespace SignalManager