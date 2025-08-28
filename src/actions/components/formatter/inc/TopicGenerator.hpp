#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <sstream>
#include <regex>
#include "ColumnConfigInstance.hpp"
#include "MemoryPool.hpp"

class TopicGenerator {
public:
    TopicGenerator(const std::string& pattern, const ColumnConfigInstanceVector& col_instances);

    std::string generate(const MemoryPool::TableBlock& data, size_t row_index) const;

private:
    struct Token {
        enum Type { TEXT, PLACEHOLDER } type;
        std::string value;
    };

    std::vector<Token> tokens_;
    std::unordered_map<std::string, size_t> col_index_map_;

    std::vector<Token> parse_pattern(const std::string& pattern) const;
    std::string get_value(const std::string& key,
                         const MemoryPool::TableBlock& data,
                         size_t row_index) const;
};