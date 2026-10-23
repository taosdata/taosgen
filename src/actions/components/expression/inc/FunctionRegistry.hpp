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

#include <lua.hpp>
#include <functional>
#include <vector>
#include <string>
#include <unordered_map>

class FunctionRegistry {
public:
    using RegisterFunction = std::function<void(lua_State*)>;

    // Singleton access
    static FunctionRegistry& instance() {
        static FunctionRegistry reg;
        return reg;
    }

    // Explicitly register a module
    void register_module(const std::string& name, RegisterFunction func) {
        modules_[name] = func;
    }

    // Get the registration function for a specific module
    RegisterFunction get_module(const std::string& name) {
        auto it = modules_.find(name);
        if (it != modules_.end()) {
            return it->second;
        }
        return nullptr;
    }

    // Register all modules to the Lua environment
    void register_all(lua_State* L) {
        for (auto& [name, func] : modules_) {
            (void)name;
            func(L);
        }
    }

private:
    std::unordered_map<std::string, RegisterFunction> modules_;
};