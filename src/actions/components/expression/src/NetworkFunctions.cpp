#include "NetworkFunctions.hpp"
#include <random>
#include <sstream>

// Random IPv4 generation
std::string NetworkFunctions::random_ipv4() {
    thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(0, 255);
    
    std::ostringstream oss;
    oss << dist(gen) << '.' << dist(gen) << '.' 
        << dist(gen) << '.' << dist(gen);
    return oss.str();
}

// Lua binding function
int NetworkFunctions::lua_random_ipv4(lua_State* L) {
    std::string ip = random_ipv4();
    lua_pushstring(L, ip.c_str());
    return 1;
}

// Register function
void NetworkFunctions::register_function(lua_State* L) {
    lua_pushcfunction(L, lua_random_ipv4);
    lua_setglobal(L, "rand_ipv4");
}