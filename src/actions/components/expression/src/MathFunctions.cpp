/*
 * Copyright (c) 2025 TAOS Data, Inc.
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

#include "MathFunctions.hpp"

// Square wave function implementation
double MathFunctions::square_wave(int call_index, double min, double max, int period, int offset) {
    if (period <= 0) return min;
    
    int pos = (call_index + offset) % period;
    return (pos < period / 2) ? max : min;
}

// Lua binding function
int MathFunctions::lua_square_wave(lua_State* L) {
    lua_getglobal(L, "_i");
    int call_index = lua_tointeger(L, -1);
    lua_pop(L, 1);
    
    double min = luaL_checknumber(L, 1);
    double max = luaL_checknumber(L, 2);
    int period = luaL_checkinteger(L, 3);
    int offset = luaL_checkinteger(L, 4);
    
    double result = MathFunctions::square_wave(call_index, min, max, period, offset);
    lua_pushnumber(L, result);
    return 1;
}

// Registration function implementation
void register_MathFunctions(lua_State* L) {
    lua_pushcfunction(L, MathFunctions::lua_square_wave);
    lua_setglobal(L, "square");
}