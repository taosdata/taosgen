#pragma once

#include "ConnectionInfo.hpp"
#include "DataFormat.hpp"
#include "DataChannel.hpp"
#include "DatabaseInfo.hpp"
#include "SuperTableInfo.hpp"
#include "ChildTableInfo.hpp"

struct CreateChildTableConfig {
    ConnectionInfo connection_info;  // Database connection info
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;      // Database info
    SuperTableInfo super_table_info; // Super table info
    ChildTableInfo child_table_info; // Child table info

    struct BatchConfig {
        int size = 1000;       // Number of child tables per batch
        int concurrency = 10;  // Number of concurrent batches
    } batch;
};
