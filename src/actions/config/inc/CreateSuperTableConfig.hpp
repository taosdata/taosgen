#pragma once

#include "TDengineConfig.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"

struct CreateSuperTableConfig {
    TDengineConfig connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};
