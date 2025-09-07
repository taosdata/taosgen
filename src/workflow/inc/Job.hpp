#pragma once

#include "Step.hpp"
#include "TDengineConfig.hpp"
#include "MqttConfig.hpp"
#include "SchemaConfig.hpp"
#include <string>
#include <vector>

struct Job {
    std::string key;                // Job identifier
    std::string name;               // Job name
    std::vector<std::string> needs; // Dependent jobs
    std::vector<Step> steps;        // Steps in the job

    bool find_create = false;
    TDengineConfig tdengine;
    MqttConfig mqtt;;
    SchemaConfig schema;

    Job() = default;
    Job(const std::string& key,
        const std::string& name,
        const std::vector<std::string>& needs,
        const std::vector<Step>& steps)
        : key(key), name(name), needs(needs), steps(steps) {}
};