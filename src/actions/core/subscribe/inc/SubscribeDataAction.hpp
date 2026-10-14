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

#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "SubscribeDataConfig.hpp"
#include "LogUtils.hpp"
#include <iostream>

class SubscribeDataAction : public ActionBase {
public:
    explicit SubscribeDataAction(const GlobalConfig& global, const SubscribeDataConfig& config) : global_(global), config_(config) {}

    void execute() override {
        std::ostringstream oss;
        oss << "Subscribing to data from topics: ";
        for (const auto& topic : config_.control.subscribe_control.topics) {
            oss << topic.name << " ";
        }
        LogUtils::info("Subscribing to data from topics: {}", oss.str());

        // Implement the specific data subscription logic here
    }

private:
    const GlobalConfig& global_;
    SubscribeDataConfig config_;

    // Register SubscribeDataAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/subscribe",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<SubscribeDataAction>(global, std::get<SubscribeDataConfig>(config));
            });
        return true;
    }();
};