#pragma once
#include <lua.hpp>

class ILuaFunction {
public:
    virtual ~ILuaFunction() = default;
    virtual void register_function(lua_State* L) = 0;
    
    static int get_call_count(lua_State* L) {
        lua_getglobal(L, "__call_count");
        int call_count = lua_tointeger(L, -1);
        lua_pop(L, 1);
        return call_count;
    }
};