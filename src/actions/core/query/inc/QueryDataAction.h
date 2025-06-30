#pragma once
#include "ActionBase.h"
#include "ActionFactory.h"
#include "QueryDataConfig.h"
#include <iostream>

class QueryDataAction : public ActionBase {
public:
    explicit QueryDataAction(const GlobalConfig& global, const QueryDataConfig& config) : global_(global), config_(config) {}

    void execute() override {
        std::cout << "Querying data from database: " << config_.source.connection_info.host << std::endl;
        // 在此处实现具体的数据查询逻辑
    }

private:
    const GlobalConfig& global_;
    QueryDataConfig config_;

    // 注册 QueryDataAction 到 ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/query-data",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<QueryDataAction>(global, std::get<QueryDataConfig>(config));
            });
        return true;
    }();
};