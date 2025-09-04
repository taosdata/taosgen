#pragma once

#include "TDengineConfig.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"

struct CreateDatabaseConfig {
    TDengineConfig connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
};
