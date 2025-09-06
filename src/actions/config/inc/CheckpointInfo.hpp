#pragma once
#include <cstdint>

struct CheckpointInfo {
    bool enabled = false;
    int interval_sec = 60; // Checkpoint interval in seconds
};