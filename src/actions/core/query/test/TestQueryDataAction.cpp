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

#include "QueryDataAction.hpp"
#include <cassert>
#include <iostream>

QueryDataConfig create_test_query_config() {
    QueryDataConfig cfg;
    cfg.source.connection_info.host = "localhost";
    return cfg;
}

void test_query_execute() {
    GlobalConfig global;
    auto cfg = create_test_query_config();

    try {
        QueryDataAction action(global, cfg);
        action.execute();
        std::cout << "test_query_execute passed\n";
    } catch (...) {
        assert(false && "QueryDataAction::execute should not throw");
    }
}

int main() {
    test_query_execute();
    std::cout << "All QueryDataAction tests passed.\n";
    return 0;
}