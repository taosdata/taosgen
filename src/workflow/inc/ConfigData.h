#pragma once

#include <vector>
#include "GlobalConfig.h"
#include "Job.h"

// Top-level config
struct ConfigData {
    GlobalConfig global;
    int concurrency = 1;
    std::vector<Job> jobs; // Store job list
};