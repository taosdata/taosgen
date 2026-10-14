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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <functional>
#include "ActionConfigVariant.hpp"
#include "ActionBase.hpp"
#include "GlobalConfig.hpp"

class ActionFactory {
public:
    using ActionCreator = std::function<std::unique_ptr<ActionBase>(const GlobalConfig&, const ActionConfigVariant&)>;

    static ActionFactory& instance() {
        static ActionFactory factory;
        return factory;
    }

    void register_action(const std::string& action_type, ActionCreator creator) {
        std::lock_guard<std::mutex> lock(mutex_);
        creators_[action_type] = std::move(creator);
    }

    std::unique_ptr<ActionBase> create_action(const GlobalConfig& global, const std::string& action_type, const ActionConfigVariant& config) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = creators_.find(action_type);
        if (it != creators_.end()) {
            return it->second(global, config);
        }
        throw std::invalid_argument("Unsupported action type: " + action_type);
    }

private:
    std::unordered_map<std::string, ActionCreator> creators_;
    std::mutex mutex_;
};