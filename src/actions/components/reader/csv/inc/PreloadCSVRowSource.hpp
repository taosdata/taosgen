#pragma once

#include "ICSVRowSource.hpp"
#include "CSVDataManager.hpp"
#include <vector>
#include <memory>

class PreloadCSVRowSource : public ICSVRowSource {
public:
    // Constructor using shared rows (default table, tbname_index = -1)
    explicit PreloadCSVRowSource(CSVDataManager::SharedRows shared_rows)
        : shared_rows_(std::move(shared_rows)), row_index_(0) {}

    // Constructor using owned rows (specific table)
    explicit PreloadCSVRowSource(std::vector<RowData> rows)
        : owned_rows_(std::move(rows)), row_index_(0) {}

    std::optional<RowData> next() override {
        const auto& rows = get_rows();
        if (rows.empty()) return std::nullopt;
        if (row_index_ >= rows.size()) return std::nullopt;

        auto& row = rows[row_index_];
        row_index_ = (row_index_ + 1) % rows.size();
        return row;
    }

    bool has_more() const override {
        return !get_rows().empty();
    }

    void reset() override {
        row_index_ = 0;
    }

    size_t total_rows() const override {
        return get_rows().size();
    }

    // Direct access to underlying rows (for backward compatibility)
    const std::vector<RowData>& rows() const { return get_rows(); }
    size_t current_index() const { return row_index_; }
    void set_index(size_t idx) { row_index_ = idx; }

private:
    const std::vector<RowData>& get_rows() const {
        if (shared_rows_) return *shared_rows_;
        return owned_rows_;
    }

    CSVDataManager::SharedRows shared_rows_;
    std::vector<RowData> owned_rows_;
    size_t row_index_ = 0;
};
