#include "StepExecutionStrategy.hpp"
#include <iostream>
#include "ActionFactory.hpp"
#include "CreateDatabaseAction.hpp"
#include "CreateSuperTableAction.hpp"
#include "CreateChildTableAction.hpp"
#include "InsertDataAction.hpp"
#include "QueryDataAction.hpp"
#include "SubscribeDataAction.hpp"


// Implementation of production environment strategy
void ProductionStepStrategy::execute(const Step& step) {
    try {
        std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

        auto action = ActionFactory::instance().create_action(global_, step.uses, step.action_config);
        action->execute();

        std::cout << "Step completed: " << step.name << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error executing step: " << step.name << " (" << step.uses << ")" << std::endl;
        std::cerr << "Reason: Exception - " << e.what() << std::endl;
        throw;
    }
}


// Implementation of debug environment strategy
void DebugStepStrategy::execute(const Step& step) {
    std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

    if (step.uses == "tdengine/create-database") {
        std::cout << "Action type: Create Database" << std::endl;
    } else if (step.uses == "tdengine/create-super-table") {
        std::cout << "Action type: Create Super Table" << std::endl;
    } else if (step.uses == "actions/create-child-table") {
        std::cout << "Action type: Create Child Table" << std::endl;
    } else if (step.uses == "actions/insert-data") {
        std::cout << "Action type: Insert Data" << std::endl;
    } else if (step.uses == "actions/query-data") {
        std::cout << "Action type: Query Data" << std::endl;
    } else if (step.uses == "actions/subscribe-data") {
        std::cout << "Action type: Subscribe Data" << std::endl;
    } else {
        std::cerr << "Unknown action type: " << step.uses << std::endl;
        throw std::runtime_error("Unknown action type: " + step.uses);
    }

    std::cout << "Step completed: " << step.name << std::endl;
}