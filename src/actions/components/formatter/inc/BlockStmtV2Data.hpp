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

#include "IStmtData.hpp"
#include "MemoryPool.hpp"


class BlockStmtV2Data : public IStmtData {
public:
    BlockStmtV2Data(MemoryPool::MemoryBlock* block,
                   const ColumnConfigInstanceVector& col_instances)
        : block_(block), col_instances_(col_instances) {}

    size_t row_count() const noexcept override {
        return block_ ? block_->total_rows : 0;
    }

    size_t column_count() const noexcept override {
        return col_instances_.size();
    }

    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept override {
        return block_ ? &block_->bindv_ : nullptr;
    }

    MemoryPool::MemoryBlock* get_block() const noexcept {
        return block_;
    }

    void reset_block() noexcept {
        block_ = nullptr;
    }

private:
    MemoryPool::MemoryBlock* block_;
    const ColumnConfigInstanceVector& col_instances_;
};