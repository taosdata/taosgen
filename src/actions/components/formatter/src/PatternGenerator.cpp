#include "PatternGenerator.hpp"

std::vector<PatternGenerator::Token> PatternGenerator::parse_pattern(const std::string& pattern) {
    std::vector<Token> tokens;
    std::regex re(R"(\{([^}]+)\})");
    std::sregex_iterator it(pattern.begin(), pattern.end(), re);
    std::sregex_iterator end;

    std::ptrdiff_t last_pos = 0;
    while (it != end) {
        if (it->position() > last_pos) {
            tokens.push_back({
                Token::TEXT,
                pattern.substr(static_cast<size_t>(last_pos), static_cast<size_t>(it->position() - last_pos))
            });
        }

        tokens.push_back({
            Token::PLACEHOLDER,
            it->str(1)
        });

        last_pos = it->position() + it->length();
        ++it;
    }

    if (last_pos < static_cast<std::ptrdiff_t>(pattern.length())) {
        tokens.push_back({
            Token::TEXT,
            pattern.substr(static_cast<size_t>(last_pos))
        });
    }

    return tokens;
}

std::string PatternGenerator::get_value_as_string(const std::string& key, const MemoryPool::TableBlock& data, size_t row_index) const {
    if (key == "table") {
        return data.table_name ? data.table_name : "UNKNOWN_TABLE";
    }
    if (key == "ts") {
        if (data.timestamps && row_index < data.used_rows) {
            return std::to_string(data.timestamps[row_index]);
        }
        return "INVALID_TS";
    }

    // Data column handling
    auto it = col_index_map_.find(key);
    if (it == col_index_map_.end()) {
        return "{COL_NOT_FOUND:" + key + "}";
    }

    try {
        return data.get_cell_as_string(row_index, it->second);
    } catch (const std::exception& e) {
        return "{ERROR:" + std::string(e.what()) + "}";
    }
}