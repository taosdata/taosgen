#include "StepExecutionStrategy.h"
#include <iostream>
#include "ActionFactory.h"
#include "CreateDatabaseAction.h"
#include "CreateSuperTableAction.h"
#include "CreateChildTableAction.h"
#include "InsertDataAction.h"
#include "QueryDataAction.h"
#include "SubscribeDataAction.h"


// 生产环境策略的实现
void ProductionStepStrategy::execute(const Step& step) {
    try {
        std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

        auto action = ActionFactory::instance().create_action(step.uses, step.action_config);
        action->execute();

        std::cout << "Step completed: " << step.name << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error executing step: " << step.name << " (" << step.uses << ")" << std::endl;
        std::cerr << "Reason: Exception - " << e.what() << std::endl;
        throw;
    }
}


// 调试环境策略的实现
void DebugStepStrategy::execute(const Step& step) {
    std::cout << "Executing step: " << step.name << " (" << step.uses << ")" << std::endl;

    if (step.uses == "actions/create-database") {
        std::cout << "Action type: Create Database" << std::endl;
    } else if (step.uses == "actions/create-super-table") {
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
