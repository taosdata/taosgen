#pragma once
#include <taosws.h>
#include "DatabaseConnector.hpp"

class WebsocketConnector final : public DatabaseConnector {
public:
    explicit WebsocketConnector(const ConnectionInfo& conn_info);
    ~WebsocketConnector() override;

    bool connect() override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;
    void close() noexcept override;

private:
    WS_TAOS* conn_{nullptr};
    ConnectionInfo conn_info_;
    bool is_connected_{false};
};