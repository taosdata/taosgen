#include "StepExecutionStrategy.hpp"
#include "ActionFactory.hpp"
#include "CreateDatabaseAction.hpp"
#include "CreateSuperTableAction.hpp"
#include "CreateChildTableAction.hpp"
#include "InsertDataAction.hpp"
#include "QueryDataAction.hpp"
#include "SubscribeDataAction.hpp"
#include <iostream>
#include <mutex>


// Implementation of production environment strategy
bool ProductionStepStrategy::execute(const Step& step) {
    try {
        std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

        auto action = ActionFactory::instance().create_action(global_, step.uses, step.action_config);
        action->execute();

        std::cout << "Step completed: " << step.name << std::endl;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error executing step: " << step.name << ", "
                  << "reason: \"Exception - " << e.what() << "\"" << std::endl;
        return false;
    }
}


// Implementation of debug environment strategy
static std::mutex log_mutex;
bool DebugStepStrategy::execute(const Step& step) {
    std::lock_guard<std::mutex> lock(log_mutex);

    std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

    if (step.uses == "tdengine/create-database") {
        std::cout << "Action type: Create Database" << std::endl;
    } else if (step.uses == "tdengine/create-super-table") {
        std::cout << "Action type: Create Super Table" << std::endl;
    } else if (step.uses == "tdengine/create-child-table") {
        std::cout << "Action type: Create Child Table" << std::endl;
    } else if (step.uses == "tdengine/insert-data") {
        std::cout << "Action type: Insert Data" << std::endl;
    } else if (step.uses == "actions/query-data") {
        std::cout << "Action type: Query Data" << std::endl;
    } else if (step.uses == "actions/subscribe-data") {
        std::cout << "Action type: Subscribe Data" << std::endl;
    } else {
        std::cerr << "Unknown action type: " << step.uses << std::endl;
        return false;
    }

    std::cout << "Step completed: " << step.name << std::endl;
    return true;
}