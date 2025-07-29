#pragma once
#include <taos.h>
#include "DatabaseConnector.hpp"

class RestfulConnector final : public DatabaseConnector {
public:
    explicit RestfulConnector(const ConnectionInfo& conn_info);
    ~RestfulConnector() override;

    bool connect() override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;
    void close() noexcept override;

private:

    ConnectionInfo conn_info_;
    bool is_connected_{false};
};