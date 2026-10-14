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

#include <vector>
#include <string>
#include "IStmtData.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"

class StmtV2Data : public IStmtData {
public:
    explicit StmtV2Data(const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch);
    StmtV2Data(StmtV2Data&& other) noexcept;
    StmtV2Data& operator=(StmtV2Data&& other) = delete;
    StmtV2Data(const StmtV2Data&) = delete;
    StmtV2Data& operator=(const StmtV2Data&) = delete;

    size_t row_count() const noexcept override;
    size_t column_count() const noexcept override;
    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept override;

private:
    struct ColumnMemory {
        std::vector<char> buffer;
        std::vector<int32_t> lengths;
        std::vector<char> is_nulls;
    };
    struct TableMemory {
        std::vector<ColumnMemory> column_data;
    };

    MultiBatch batch_;
    const ColumnConfigInstanceVector& col_instances_;
    std::vector<const char*> table_names_;
    std::vector<std::vector<TAOS_STMT2_BIND>> column_binds_;
    std::vector<TAOS_STMT2_BIND*> column_bind_ptrs_;
    std::vector<TableMemory> table_memories_;
    TAOS_STMT2_BINDV bindv_{};
};