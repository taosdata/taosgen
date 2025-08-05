#pragma once
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "SubscribeDataConfig.hpp"
#include <iostream>

class SubscribeDataAction : public ActionBase {
public:
    explicit SubscribeDataAction(const GlobalConfig& global, const SubscribeDataConfig& config) : global_(global), config_(config) {}

    void execute() override {
        std::cout << "Subscribing to data from topics: ";
        for (const auto& topic : config_.control.subscribe_control.topics) {
            std::cout << topic.name << " ";
        }
        std::cout << std::endl;
        // Implement the specific data subscription logic here
    }

private:
    const GlobalConfig& global_;
    SubscribeDataConfig config_;

    // Register SubscribeDataAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/subscribe-data",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<SubscribeDataAction>(global, std::get<SubscribeDataConfig>(config));
            });
        return true;
    }();
};