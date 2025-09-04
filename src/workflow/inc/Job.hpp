#pragma once

#include <string>
#include <vector>
#include "Step.hpp"

struct Job {
    std::string key;                // Job identifier
    std::string name;               // Job name
    std::vector<std::string> needs; // Dependent jobs
    std::vector<Step> steps;        // Steps in the job
    bool find_create = false;
};
