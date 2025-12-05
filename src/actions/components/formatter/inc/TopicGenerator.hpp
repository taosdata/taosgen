#pragma once
#include "PatternGenerator.hpp"

class TopicGenerator final : public PatternGenerator {
public:
    TopicGenerator(const std::string& pattern, const ColumnConfigInstanceVector& col_instances);

    std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const override;
};