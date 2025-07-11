#pragma once
#include <iostream>
#include "ActionBase.h"
#include "ActionFactory.h"
#include "CreateChildTableConfig.h"
#include "DatabaseConnector.h"


class CreateChildTableAction : public ActionBase {
public:
    explicit CreateChildTableAction(const GlobalConfig& global, const CreateChildTableConfig& config) : global_(global), config_(config) {}

    void execute() override;

private:
    const GlobalConfig& global_;
    CreateChildTableConfig config_;

    // Register CreateChildTableAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/create-child-table",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CreateChildTableAction>(global, std::get<CreateChildTableConfig>(config));
            });
        return true;
    }();
};