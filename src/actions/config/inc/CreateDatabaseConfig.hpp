#pragma once

#include "ConnectionInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"
#include "CheckpointInfo.hpp"

struct CreateDatabaseConfig {
    ConnectionInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    CheckpointInfo checkpoint_info;
};
