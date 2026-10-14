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

#include "ScopedEnvVar.hpp"
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

namespace {
std::string unique_env_name(const std::string& base) {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    return base + "_" + std::to_string(now) + "_" + std::to_string(tid);
}

std::string get_env_value(const std::string& name, bool& has_value) {
    const char* value = std::getenv(name.c_str());
    if (value) {
        has_value = true;
        return std::string(value);
    }
    has_value = false;
    return {};
}

void set_env_value(const std::string& name, const std::string& value) {
#ifdef _WIN32
    _putenv_s(name.c_str(), value.c_str());
#else
    setenv(name.c_str(), value.c_str(), 1);
#endif
}

void unset_env_value(const std::string& name) {
#ifdef _WIN32
    // On Windows, _putenv_s with empty string removes the variable from CRT environment
    _putenv_s(name.c_str(), "");
#else
    unsetenv(name.c_str());
#endif
}

bool env_is_unset(const std::string& name) {
    const char* value = std::getenv(name.c_str());
#ifdef _WIN32
    // On Windows, _putenv_s("name", "") sets to empty string rather than removing.
    // Treat both nullptr and empty string as "unset".
    return (value == nullptr || value[0] == '\0');
#else
    return (value == nullptr);
#endif
}
}

void test_empty_name_throws() {
    bool threw = false;
    try {
        ScopedEnvVar env("", "value");
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    (void)threw;
    assert(threw);
    std::cout << "test_empty_name_throws passed\n";
}

void test_set_and_restore_new_var() {
    const std::string name = unique_env_name("TSGEN_TEST_ENV_NEW");
    unset_env_value(name);

    {
        ScopedEnvVar env(name, "new_value");
        bool has_value = false;
        auto value = get_env_value(name, has_value);
        assert(has_value);
        assert(value == "new_value");
    }

    bool has_value = false;
    (void)get_env_value(name, has_value);
    bool is_unset = env_is_unset(name);
    (void)is_unset;
    assert(is_unset);
    std::cout << "test_set_and_restore_new_var passed\n";
}

void test_restore_old_value() {
    const std::string name = unique_env_name("TSGEN_TEST_ENV_OLD");
    set_env_value(name, "old_value");

    {
        ScopedEnvVar env(name, "new_value");
        bool has_value = false;
        auto value = get_env_value(name, has_value);
        assert(has_value);
        assert(value == "new_value");

        env.restore();
        value = get_env_value(name, has_value);
        assert(has_value);
        assert(value == "old_value");
    }

    bool has_value = false;
    auto value = get_env_value(name, has_value);
    assert(has_value);
    assert(value == "old_value");

    unset_env_value(name);
    std::cout << "test_restore_old_value passed\n";
}

void test_manual_restore_without_old_value() {
    const std::string name = unique_env_name("TSGEN_TEST_ENV_NONE");
    unset_env_value(name);

    {
        ScopedEnvVar env(name, "temp_value");
        env.restore();
        bool has_value = false;
        (void)get_env_value(name, has_value);
        assert(!has_value);
    }

    bool has_value = false;
    (void)get_env_value(name, has_value);
    assert(!has_value);
    std::cout << "test_manual_restore_without_old_value passed\n";
}

int main() {
    test_empty_name_throws();
    test_set_and_restore_new_var();
    test_restore_old_value();
    test_manual_restore_without_old_value();

    std::cout << "All ScopedEnvVar tests passed.\n";
    return 0;
}
