#pragma once
#include "FunctionRegistry.hpp"
#include <lua.hpp>

class MathFunctions {
public:
    static double square_wave(int call_count, double min, double max, int period, int offset);

    static int lua_square_wave(lua_State* L);
};

// Declare registration function
void register_MathFunctions(lua_State* L);