#pragma once
#include "ILuaFunction.hpp"

class MathFunctions : public ILuaFunction {
public:
    void register_function(lua_State* L) override;
    
    static double square_wave(int call_count, double min, double max, int period, int offset);
    
private:
    static int lua_square_wave(lua_State* L);
};