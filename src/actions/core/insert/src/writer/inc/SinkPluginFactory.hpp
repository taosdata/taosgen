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

#include "ISinkPlugin.hpp"
#include "InsertDataConfig.hpp"
#include "ActionRegisterInfo.hpp"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <functional>

class SinkPluginFactory {
public:
    using Builder = std::function<std::unique_ptr<ISinkPlugin>(
        const InsertDataConfig&,
        const ColumnConfigInstanceVector&,
        const ColumnConfigInstanceVector&,
        size_t,
        std::shared_ptr<ActionRegisterInfo>
    )>;

    static SinkPluginFactory& instance() {
        static SinkPluginFactory inst;
        return inst;
    }

    static void register_sink_plugin(const std::string& target, Builder builder) {
        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        inst.plugins_[target] = std::move(builder);
    }

    static std::unique_ptr<ISinkPlugin> create_sink_plugin(
        const InsertDataConfig& config,
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        size_t no = 0,
        std::shared_ptr<ActionRegisterInfo> action_info = nullptr
    ) {
        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        if (auto it = inst.plugins_.find(config.target_type); it != inst.plugins_.end()) {
            return it->second(config, col_instances, tag_instances, no, std::move(action_info));
        }
        throw std::invalid_argument("Unsupported sink plugin type: " + config.target_type);
    }

private:
    std::unordered_map<std::string, Builder> plugins_;
    std::mutex mutex_;
};