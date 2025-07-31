#include "ExpressionEngine.hpp"
#include "FunctionRegistry.hpp"
#include "MathFunctions.hpp"
#include "NetworkFunctions.hpp"
#include <stdexcept>

// Global function registry manager
static FunctionRegistry& get_function_registry() {
    static FunctionRegistry registry;
    static bool initialized = false;
    
    if (!initialized) {
        // Register all function modules
        registry.add_module(std::make_unique<MathFunctions>());
        registry.add_module(std::make_unique<NetworkFunctions>());
        // Add more modules...
        initialized = true;
    }
    
    return registry;
}

struct ExpressionEngine::LuaState {
    lua_State* L;
    int compiled_ref;
    
    LuaState() : L(luaL_newstate()), compiled_ref(LUA_NOREF) {
        if (!L) throw std::runtime_error("Failed to create Lua state");

        // Open base libraries
        luaL_openlibs(L);
        
        // Register all custom functions
        get_function_registry().register_all(L);
    }
    
    ~LuaState() {
        if (compiled_ref != LUA_NOREF) {
            luaL_unref(L, LUA_REGISTRYINDEX, compiled_ref);
        }
        lua_close(L);
    }
    
    void compile(const std::string& expr) {
        // Compile expression as function
        std::string full_expr = "return " + expr;
        
        if (luaL_loadstring(L, full_expr.c_str())) {
            std::string err = lua_tostring(L, -1);
            lua_pop(L, 1);
            throw std::runtime_error("Compile error: " + err);
        }

        // Save to registry
        compiled_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    
    Result execute(int call_count) {
        // Inject call count
        lua_pushinteger(L, call_count);
        lua_setglobal(L, "__call_count");
        
        // Get precompiled function
        lua_rawgeti(L, LUA_REGISTRYINDEX, compiled_ref);
        
        // Execute function
        if (lua_pcall(L, 0, 1, 0)) {
            std::string err = lua_tostring(L, -1);
            lua_pop(L, 1);
            throw std::runtime_error("Runtime error: " + err);
        }
        
        // Handle return value
        Result result;
        if (lua_isnumber(L, -1)) {
            result = lua_tonumber(L, -1);
        } else if (lua_isstring(L, -1)) {
            result = lua_tostring(L, -1);
        } else if (lua_isboolean(L, -1)) {
            result = static_cast<bool>(lua_toboolean(L, -1));
        } else {
            lua_pop(L, 1);
            throw std::runtime_error("Invalid return type");
        }
        
        lua_pop(L, 1);
        return result;
    }
};

ExpressionEngine::ExpressionEngine(const std::string& expression) 
    : expression_(expression), lua_(std::make_unique<LuaState>()) {
    lua_->compile(expression);
}

ExpressionEngine::~ExpressionEngine() = default;

ExpressionEngine::Result ExpressionEngine::evaluate() {
    call_count_++;
    return lua_->execute(call_count_);
}