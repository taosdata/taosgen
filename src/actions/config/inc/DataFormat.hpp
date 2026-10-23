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

#include "PluginExtensions.hpp"
#include <string>

using FormatOptions = PluginExtensions;

struct DataFormat {
    std::string format_type = "sql";
    bool support_tags = false;
    FormatOptions opts;
};

template<typename T>
inline const T* get_format_opt(const DataFormat& df, const std::string& key) {
    return get_plugin_config<T>(df.opts, key);
}

template<typename T>
inline T* get_format_opt_mut(DataFormat& df, const std::string& key) {
    return get_plugin_config_mut<T>(df.opts, key);
}

template<typename T>
inline void set_format_opt(DataFormat& df, const std::string& key, T&& value) {
    set_plugin_config(df.opts, key, std::forward<T>(value));
}
