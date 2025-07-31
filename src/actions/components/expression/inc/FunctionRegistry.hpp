#pragma once
#include "ILuaFunction.hpp"
#include <vector>
#include <memory>
#include <lua.hpp>

class FunctionRegistry {
public:
    // Add function module
    void add_module(std::unique_ptr<ILuaFunction> module) {
        modules.push_back(std::move(module));
    }

    // Register all functions
    void register_all(lua_State* L) {
        for (auto& module : modules) {
            module->register_function(L);
        }
    }

private:
    std::vector<std::unique_ptr<ILuaFunction>> modules;
};