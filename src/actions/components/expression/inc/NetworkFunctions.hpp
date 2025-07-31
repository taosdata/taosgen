#pragma once
#include "ILuaFunction.hpp"
#include <string>

class NetworkFunctions : public ILuaFunction {
public:
    void register_function(lua_State* L) override;

    static std::string random_ipv4();
    
private:
    static int lua_random_ipv4(lua_State* L);
};