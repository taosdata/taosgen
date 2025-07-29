
#include "ColumnGeneratorFactory.hpp"
#include <stdexcept>
#include "RandomColumnGenerator.hpp"
#include "OrderColumnGenerator.hpp"
#include "FunctionColumnGenerator.hpp"


std::unique_ptr<ColumnGenerator> ColumnGeneratorFactory::create(const ColumnConfigInstance& instance) {
    if (!instance.config().gen_type) return nullptr;
    
    const std::string& gen_type = *instance.config().gen_type;
    
    if (gen_type == "random") {
        return std::make_unique<RandomColumnGenerator>(instance);
    } 
    else if (gen_type == "order") {
        return std::make_unique<OrderColumnGenerator>(instance);
    }
    else if (gen_type == "function") {
        return std::make_unique<FunctionColumnGenerator>(instance);
    }
    
    throw std::runtime_error("Unsupported generator type: " + gen_type);
}