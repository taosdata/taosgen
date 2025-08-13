#pragma once

#include "DatabaseConnector.hpp"
#include <memory>

class ConnectorFactory {
public:
    static std::unique_ptr<DatabaseConnector> create(const DataChannel& channel, const ConnectionInfo& connInfo);
};