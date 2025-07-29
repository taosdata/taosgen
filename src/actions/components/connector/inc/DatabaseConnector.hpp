#pragma once
#include <memory>
#include <string>
#include "CreateDatabaseConfig.hpp"
#include "FormatResult.hpp"


class DatabaseConnector {
public:
    virtual ~DatabaseConnector() = default;

    virtual bool connect() = 0;
    virtual bool select_db(const std::string& db_name) = 0;
    virtual bool prepare(const std::string& sql) = 0;
    virtual bool execute(const std::string& sql) = 0;
    virtual bool execute(const SqlInsertData& data) = 0;
    virtual bool execute(const StmtV2InsertData& data) = 0;
    virtual void close() noexcept = 0;

    // Factory method with error handling
    static std::unique_ptr<DatabaseConnector> create(
        const DataChannel& channel,
        const ConnectionInfo& connInfo);
};