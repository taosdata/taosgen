#pragma once
#include "ColumnType.hpp"
#include <string>
#include <variant>
#include <memory>
#include <random>
#include <mutex>

class ExpressionEngine {
public:
    // using Result = std::variant<double, std::string>;
    using Result = ColumnType;

    explicit ExpressionEngine(const std::string& expression);
    ~ExpressionEngine();
    
    Result evaluate();
    
    // Disable copy and move
    ExpressionEngine(const ExpressionEngine&) = delete;
    ExpressionEngine& operator=(const ExpressionEngine&) = delete;

private:
    const std::string& expression_;
    struct LuaState;
    std::unique_ptr<LuaState> lua_;
    int call_count_ = 0;
};