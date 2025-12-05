#include "TopicGenerator.hpp"

TopicGenerator::TopicGenerator(const std::string& pattern,
                               const ColumnConfigInstanceVector& col_instances) {
    // 1. Parse the template
    tokens_ = parse_pattern(pattern);

    // 2. Build a mapping from column name to index
    for (size_t i = 0; i < col_instances.size(); i++) {
        col_index_map_[col_instances[i].name()] = i;
    }
}

std::string TopicGenerator::generate(const MemoryPool::TableBlock& data,
                                     size_t row_index) const {
    fmt::memory_buffer buffer;

    for (const auto& token : tokens_) {
        if (token.type == Token::TEXT) {
            fmt::format_to(std::back_inserter(buffer), "{}", token.value);
        } else {
            fmt::format_to(std::back_inserter(buffer), "{}", get_value_as_string(token.value, data, row_index));
        }
    }
    return fmt::to_string(buffer);
}