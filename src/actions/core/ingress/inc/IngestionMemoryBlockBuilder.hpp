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

#include "IngestionValidator.hpp"
#include "MemoryPool.hpp"
#include "SchemaConfig.hpp"
#include <mutex>

namespace taosgen::ingress {
class IngestRequest;
class InterlaceBatch;
class TableBatch;
class TimestampValues;
class ColumnValues;
class TableNames;
} // namespace taosgen::ingress

class IngestionMemoryBlockBuilder {
public:
    IngestionMemoryBlockBuilder(SchemaConfig schema, IngressConfig config);

    // Direct/internal request APIs are retained for non-gRPC callers and tests.
    IngestValidationResult validate(const IngestRequestData& request) const;
    MemoryPool::MemoryBlock* build(const IngestRequestData& request, MemoryPool& pool) const;
    MemoryPool::MemoryBlock* build(IngestRequestData&& request, MemoryPool& pool) const;

    // accept_only uses this validation-only path. The write path intentionally
    // validates while filling the block to avoid a second full request walk.
    IngestValidationResult validate_proto(const taosgen::ingress::IngestRequest& request) const;
    MemoryPool::MemoryBlock* build_proto(const taosgen::ingress::IngestRequest& request, MemoryPool& pool) const;

    const ColumnConfigInstanceVector& column_instances() const noexcept {
        return col_instances_;
    }

    const ColumnConfigInstanceVector& tag_instances() const noexcept {
        return tag_instances_;
    }

    const SchemaConfig& schema() const noexcept {
        return schema_;
    }

private:
    struct PendingTagRegistration {
        size_t table_index = 0;
        std::string table_name;
        RowType values;
    };

    SchemaConfig schema_;
    IngressConfig config_;
    IngestionValidator validator_;
    ColumnConfigInstanceVector col_instances_;
    ColumnConfigInstanceVector tag_instances_;
    mutable std::once_flag table_name_cache_once_;
    mutable std::vector<std::string> generated_table_name_cache_;

    void validate_schema_for_builder() const;
    void fill_table(MemoryPool::MemoryBlock& block,
                    size_t table_index,
                    const IngestTableBatch& table,
                    MemoryPool& pool,
                    std::vector<PendingTagRegistration>& pending_tags) const;
    void fill_table(MemoryPool::MemoryBlock& block,
                    size_t table_index,
                    IngestTableBatch& table,
                    MemoryPool& pool,
                    bool move_table_name,
                    std::vector<PendingTagRegistration>& pending_tags) const;
    void fill_primary_timestamps(MemoryPool::MemoryBlock& block,
                                 MemoryPool::TableBlock& table_block,
                                 const IngestTimestampValues& timestamps) const;
    void fill_column(MemoryPool::TableBase::Column& column_block,
                     const IngestColumnValues& values,
                     const ColumnConfig& column_config,
                     const ColumnConfigInstance& column_instance,
                     size_t row_count) const;
    void fill_proto_table(MemoryPool::MemoryBlock& block,
                          size_t table_index,
                          const taosgen::ingress::TableBatch& table,
                          size_t& total_rows,
                          std::vector<PendingTagRegistration>& pending_tags) const;
    void fill_proto_primary_timestamps(MemoryPool::MemoryBlock& block,
                                       MemoryPool::TableBlock& table_block,
                                       const taosgen::ingress::TimestampValues& timestamps) const;
    void fill_proto_column(MemoryPool::TableBase::Column& column_block,
                           const taosgen::ingress::ColumnValues& values,
                           const ColumnConfig& column_config,
                           const ColumnConfigInstance& column_instance,
                           size_t row_count) const;
    void fill_proto_interlace_batch(MemoryPool::MemoryBlock& block,
                                    const taosgen::ingress::InterlaceBatch& batch,
                                    MemoryPool& pool) const;
    void fill_proto_interlace_primary_timestamps(MemoryPool::MemoryBlock& block,
                                                 const taosgen::ingress::InterlaceBatch& batch,
                                                 size_t table_count,
                                                 size_t rows_per_table) const;
    void fill_proto_interlace_column(const taosgen::ingress::ColumnValues& values,
                                     const ColumnConfig& column_config,
                                     const ColumnConfigInstance& column_instance,
                                     size_t column_index,
                                     MemoryPool::MemoryBlock& block,
                                     size_t table_count,
                                     size_t rows_per_table) const;
    void set_proto_interlace_table_name(MemoryPool::MemoryBlock& block,
                                        size_t table_index,
                                        const taosgen::ingress::TableNames& table_names) const;
    void commit_pending_tags(MemoryPool::MemoryBlock& block,
                             MemoryPool& pool,
                             const std::vector<PendingTagRegistration>& pending_tags) const;
    const char* generated_table_name(uint64_t table_index) const;
};
