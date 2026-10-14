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

#include "NetworkFunctions.hpp"
#include "pcg_random.hpp"
#include <random>
#include <sstream>

// Random IPv4 generator
std::string NetworkFunctions::random_ipv4() {
    // thread_local std::mt19937 gen(std::random_device{}());
    thread_local pcg32_fast gen(pcg_extras::seed_seq_from<std::random_device>{});
    std::uniform_int_distribution<int> dist(0, 255);
    
    std::ostringstream oss;
    oss << dist(gen) << '.' << dist(gen) << '.' 
        << dist(gen) << '.' << dist(gen);
    return oss.str();
}

// Lua binding function - random IPv4
int NetworkFunctions::lua_random_ipv4(lua_State* L) {
    std::string ip = random_ipv4();
    lua_pushstring(L, ip.c_str());
    return 1;
}


// Registration function implementation
void register_NetworkFunctions(lua_State* L) {
    // Register random IPv4 function
    lua_pushcfunction(L, NetworkFunctions::lua_random_ipv4);
    lua_setglobal(L, "rand_ipv4");
}