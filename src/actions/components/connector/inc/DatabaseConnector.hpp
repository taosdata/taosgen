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

#include "CreateDatabaseConfig.hpp"
#include "FormatResult.hpp"
#include "SqlInsertData.hpp"
#include "StmtV2InsertData.hpp"
#include "SchemalessInsertData.hpp"
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

struct ConnectorError {
    std::optional<int64_t> code;
    std::string message;
};

class DatabaseConnector {
public:
    virtual ~DatabaseConnector() = default;

    // Connection management
    virtual bool connect() = 0;
    virtual void close() noexcept = 0;

    // Database operations
    virtual bool select_db(const std::string& db_name) = 0;
    virtual bool prepare(const std::string& sql) = 0;
    virtual bool execute(const std::string& sql) = 0;
    virtual bool execute(const SqlInsertData& data) = 0;
    virtual bool execute(const StmtV2InsertData& data) = 0;
    virtual bool execute(const SchemalessInsertData& data) = 0;

    // State management
    virtual bool is_connected() const = 0;
    virtual bool is_valid() const = 0;
    virtual void reset_state() noexcept = 0;

    virtual const ConnectorError& last_error() const noexcept { return last_error_; }

protected:
    void clear_last_error() const noexcept {
        last_error_ = {};
    }

    void set_last_error(std::optional<int64_t> code, std::string message) const {
        last_error_.code = code;
        last_error_.message = std::move(message);
    }

private:
    mutable ConnectorError last_error_;
};
