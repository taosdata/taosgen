#include "MathFunctions.hpp"

// Square wave function implementation
double MathFunctions::square_wave(int call_count, double min, double max, int period, int offset) {
    if (period <= 0) return min;
    
    int pos = (call_count - 1 + offset) % period;
    return (pos < period / 2) ? max : min;
}

// Lua binding function
int MathFunctions::lua_square_wave(lua_State* L) {
    double min = luaL_checknumber(L, 1);
    double max = luaL_checknumber(L, 2);
    int period = luaL_checkinteger(L, 3);
    int offset = luaL_checkinteger(L, 4);
    
    int call_count = get_call_count(L);
    double result = square_wave(call_count, min, max, period, offset);
    lua_pushnumber(L, result);
    return 1;
}

// Register function
void MathFunctions::register_function(lua_State* L) {
    lua_pushcfunction(L, lua_square_wave);
    lua_setglobal(L, "square");
}