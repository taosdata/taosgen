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

#include "SubscribeDataAction.hpp"
#include <cassert>
#include <iostream>

SubscribeDataConfig create_test_subscribe_config() {
    SubscribeDataConfig cfg;
    cfg.control.subscribe_control.topics.push_back({ "topic1", "select * from st" });
    cfg.control.subscribe_control.topics.push_back({ "topic2", "select count(*) from st" });
    return cfg;
}

void test_subscribe_execute() {
    GlobalConfig global;
    auto cfg = create_test_subscribe_config();

    try {
        SubscribeDataAction action(global, cfg);
        action.execute();
        std::cout << "test_subscribe_execute passed\n";
    } catch (...) {
        assert(false && "SubscribeDataAction::execute should not throw");
    }
}

int main() {
    test_subscribe_execute();
    std::cout << "All SubscribeDataAction tests passed.\n";
    return 0;
}