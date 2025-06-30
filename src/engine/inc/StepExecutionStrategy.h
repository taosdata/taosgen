#pragma once

#include "Step.h"
#include "GlobalConfig.h"


// 抽象基类：步骤执行策略
class StepExecutionStrategy {
public:
    StepExecutionStrategy(const GlobalConfig& global) : global_(global) {}

    virtual ~StepExecutionStrategy() = default;

    // 执行步骤的接口
    virtual void execute(const Step& step) = 0;

protected:
    const GlobalConfig& global_;
};

// 生产环境策略
class ProductionStepStrategy : public StepExecutionStrategy {
public:
    ProductionStepStrategy(const GlobalConfig& global) : StepExecutionStrategy(global) {}

    void execute(const Step& step) override;
};

// 调试环境策略
class DebugStepStrategy : public StepExecutionStrategy {
public:
    DebugStepStrategy(const GlobalConfig& global) : StepExecutionStrategy(global) {}

    void execute(const Step& step) override;
};
