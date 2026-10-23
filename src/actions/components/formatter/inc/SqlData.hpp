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

#include <string>

class SqlData {
public:
    explicit SqlData(std::string&& sql_str)
        : sql_buffer_(std::move(sql_str)) 
    {
        c_str_ = sql_buffer_.data();
    }

    SqlData(const SqlData&) = delete;
    SqlData& operator=(const SqlData&) = delete;

    SqlData(SqlData&&) = default;
    SqlData& operator=(SqlData&&) = default;

    const char* c_str() const noexcept { return c_str_; }
    size_t size() const noexcept { return sql_buffer_.size(); }
    const std::string& str() const noexcept { return sql_buffer_; }

private:
    std::string sql_buffer_;
    const char* c_str_ = nullptr;
};