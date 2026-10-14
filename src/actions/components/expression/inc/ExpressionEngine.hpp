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

#include "ColumnType.hpp"
#include <memory>
#include <string>
#include <variant>
#include <unordered_map>
#include <mutex>
#include <lua.hpp>
#include <thread>

class ExpressionEngine {
public:
    using Result = ColumnType;

    explicit ExpressionEngine(const std::string& expression);
    ExpressionEngine(const std::string& table_name, const std::string& expression);
    ~ExpressionEngine() = default;

    Result evaluate();

    // Disable copy and move
    ExpressionEngine(const ExpressionEngine&) = delete;
    ExpressionEngine& operator=(const ExpressionEngine&) = delete;

private:
    // Thread-local context
    struct ThreadLocalContext {
        lua_State* lua_vm = nullptr;
        std::unordered_map<std::string, int> template_cache;

        ThreadLocalContext();
        ~ThreadLocalContext();
    };

    // Expression template
    struct ExpressionTemplate {
        std::string expression;
        int function_ref = LUA_NOREF;
    };

    // Execution state
    struct ExpressionState {
        std::shared_ptr<ExpressionTemplate> template_;
        int call_index = 0;
        std::string table_name;
        double last_value = 0.0;

        ExpressionState(std::shared_ptr<ExpressionTemplate> tpl, const std::string& tbl_name)
            : template_(std::move(tpl)), call_index(0), table_name(tbl_name) {}
    };

    std::unique_ptr<ExpressionState> state_;

    // Get thread-local context
    static ThreadLocalContext& get_thread_context();

    // Get or create expression template
    std::shared_ptr<ExpressionTemplate> get_template(
        const std::string& expression,
        ThreadLocalContext& context
    );
};