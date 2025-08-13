#pragma once

#include "DatabaseConnector.hpp"
#include <taos.h>
#include <mutex>
#include <map>

class TDengineConnector : public DatabaseConnector {
public:
    TDengineConnector(const ConnectionInfo& conn_info,
                          const std::string& driver_type,
                          const std::string& display_name);

    ~TDengineConnector() override;

    bool connect() override;
    void close() noexcept override;

    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool execute(const std::string& sql) override;
    bool execute(const SqlInsertData& data) override;
    bool execute(const StmtV2InsertData& data) override;

    bool is_connected() const override;
    bool is_valid() const;
    void reset_state() noexcept override;

protected:
    ConnectionInfo conn_info_;
    std::string driver_type_;
    std::string display_name_;

    bool is_connected_{false};
    std::string current_db_;
    std::string prepare_sql_;
    TAOS* conn_{nullptr};
    TAOS_STMT* stmt_{nullptr};

private:
    void init_driver();

    static std::mutex driver_init_mutex_;
    static std::map<std::string, std::once_flag> driver_init_flags;
};