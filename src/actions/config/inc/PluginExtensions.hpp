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

#include <any>
#include <string>
#include <unordered_map>

using PluginExtensions = std::unordered_map<std::string, std::any>;

template<typename T>
inline const T* get_plugin_config(const PluginExtensions& exts, const std::string& key) {
    auto it = exts.find(key);
    if (it == exts.end()) return nullptr;
    return std::any_cast<T>(&it->second);
}

template<typename T>
inline T* get_plugin_config_mut(PluginExtensions& exts, const std::string& key) {
    auto it = exts.find(key);
    if (it == exts.end()) return nullptr;
    return std::any_cast<T>(&it->second);
}

template<typename T>
inline void set_plugin_config(PluginExtensions& exts, const std::string& key, T&& value) {
    exts[key] = std::forward<T>(value);
}