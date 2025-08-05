#pragma once
#include <taos.h>
#include "DatabaseConnector.hpp"

class NativeConnector final : public DatabaseConnector {
public:
    explicit NativeConnector(const ConnectionInfo& conn_info);
    ~NativeConnector() override;

    bool connect() override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;
    void close() noexcept override;

private:
    TAOS* conn_{nullptr};
    TAOS_STMT* stmt_{nullptr}; 
    ConnectionInfo conn_info_;
    bool is_connected_{false};
    std::string prepare_sql_; 
};