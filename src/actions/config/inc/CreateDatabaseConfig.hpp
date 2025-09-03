#pragma once

#include "TDengineInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"

struct CreateDatabaseConfig {
    TDengineInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
};
