#pragma once

#include <string>
#include "ConnectionInfo.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"

struct GlobalConfig {
    bool confirm_prompt = false;
    bool verbose = false;
    std::string log_dir = "log/";
    std::string cfg_dir = "/etc/taos/";
    ConnectionInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
    SuperTableInfo super_table_info;
};
