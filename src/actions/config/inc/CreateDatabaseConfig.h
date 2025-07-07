#pragma once

#include "ConnectionInfo.h"
#include "DataFormat.h"
#include "DataChannel.h"
#include "DatabaseInfo.h"

struct CreateDatabaseConfig {
    ConnectionInfo connection_info;
    DataFormat data_format;
    DataChannel data_channel;
    DatabaseInfo database_info;
};
