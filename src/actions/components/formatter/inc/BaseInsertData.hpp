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
#include <cstdint>
#include <cstddef>
#include <typeindex>

struct BaseInsertData {
    std::type_index type;
    int64_t start_time;
    int64_t end_time;
    size_t total_rows;

    BaseInsertData(MemoryPool::MemoryBlock* block,
                   const ColumnConfigInstanceVector& col_instances,
                   const ColumnConfigInstanceVector& tag_instances)
        : BaseInsertData(typeid(BaseInsertData), block, col_instances, tag_instances) {}

    BaseInsertData(std::type_index t,
                   MemoryPool::MemoryBlock* block,
                   const ColumnConfigInstanceVector& col_instances,
                   const ColumnConfigInstanceVector& tag_instances)
        : type(t),
          start_time(block->start_time),
          end_time(block->end_time),
          total_rows(block->total_rows),
          block_(block),
          col_instances_(col_instances),
          tag_instances_(tag_instances) {}

    // Move constructor
    BaseInsertData(BaseInsertData&& other) noexcept
        : type(other.type),
          start_time(other.start_time),
          end_time(other.end_time),
          total_rows(other.total_rows),
          block_(other.block_),
          col_instances_(other.col_instances_),
          tag_instances_(other.tag_instances_),
          payload_(std::move(other.payload_)) {
        other.block_ = nullptr;
        other.type = typeid(BaseInsertData);
    }

    virtual ~BaseInsertData() {
        if (block_) {
            block_->release();
            block_ = nullptr;
        }
    }

    // Disable copy
    BaseInsertData(const BaseInsertData&) = delete;
    BaseInsertData& operator=(const BaseInsertData&) = delete;
    BaseInsertData& operator=(BaseInsertData&&) = delete;

    size_t row_count() const noexcept {
        return block_ ? block_->total_rows : 0;
    }

    size_t column_count() const noexcept {
        return col_instances_.size();
    }

    size_t tag_count() const noexcept {
        return tag_instances_.size();
    }

    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept {
        return block_ ? &block_->bindv_ : nullptr;
    }

    MemoryPool::MemoryBlock* get_block() const noexcept {
        return block_;
    }

    void reset_block() noexcept {
        block_ = nullptr;
    }

    // Payload API
    template<typename T>
    bool has_payload() const noexcept {
        return payload_ && payload_->type() == typeid(T);
    }

    template<typename T>
    const T* payload_as() const noexcept {
        if (!payload_ || payload_->type() != typeid(T)) return nullptr;
        return static_cast<const PayloadModel<T>*>(payload_.get())->ptr();
    }

    template<typename T>
    static std::unique_ptr<BaseInsertData> make_with_payload(
        MemoryPool::MemoryBlock* block,
        const ColumnConfigInstanceVector& col_instances,
        const ColumnConfigInstanceVector& tag_instances,
        T&& payload)
    {
        using U = std::decay_t<T>;
        auto res = std::unique_ptr<BaseInsertData>(
            new BaseInsertData(std::type_index(typeid(U)), block, col_instances, tag_instances));
        res->payload_ = std::make_unique<PayloadModel<U>>(std::forward<T>(payload));
        return res;
    }

private:
    struct PayloadConcept {
        virtual ~PayloadConcept() = default;
        virtual const std::type_info& type() const noexcept = 0;
    };

    template<typename T>
    struct PayloadModel final : PayloadConcept {
        T value;
        template<typename U>
        explicit PayloadModel(U&& v) : value(std::forward<U>(v)) {}
        const std::type_info& type() const noexcept override { return typeid(T); }
        const T* ptr() const noexcept { return &value; }
    };

    MemoryPool::MemoryBlock* block_;
    const ColumnConfigInstanceVector& col_instances_;
    const ColumnConfigInstanceVector& tag_instances_;
    std::unique_ptr<PayloadConcept> payload_;
};
