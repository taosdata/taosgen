#pragma once

#include "TDengineInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"

struct CreateSuperTableConfig {
    TDengineInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};
