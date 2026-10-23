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

#include "MemoryPool.hpp"
#include <cassert>
#include <cstdint>
#include <typeindex>

struct StmtV2InsertData {

    StmtV2InsertData(MemoryPool::MemoryBlock* block,
                     bool is_checkpoint_recover = false,
                     bool bindv_ready = false) : block_(block) {
        // bindv_ready means the caller has already built bindv, so
        // is_checkpoint_recover is intentionally ignored in that mode.
        assert(!bindv_ready || !is_checkpoint_recover);
        if (!bindv_ready) {
            block->build_bindv(is_checkpoint_recover);
        }
    }

    StmtV2InsertData(StmtV2InsertData&& other) noexcept = default;

    // Disable copy
    StmtV2InsertData(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(StmtV2InsertData&&) = delete;

    ~StmtV2InsertData() = default;

    MemoryPool::MemoryBlock* block_;
};

inline std::type_index STMTV2_TYPE_ID = std::type_index(typeid(StmtV2InsertData));
inline uint64_t STMTV2_TYPE_HASH = STMTV2_TYPE_ID.hash_code();
