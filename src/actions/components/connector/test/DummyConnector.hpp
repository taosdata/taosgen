#pragma once

#include "DatabaseConnector.hpp"
#include <memory>
#include <string>

struct DummyConnector : public DatabaseConnector {
    bool valid = true;
    bool connected = true;
    bool connect() override { return true; }
    void close() noexcept override {}
    bool select_db(const std::string&) override { return true; }
    bool prepare(const std::string&) override { return true; }
    bool execute(const std::string&) override { return true; }
    bool execute(const SqlInsertData&) override { return true; }
    bool execute(const StmtV2InsertData&) override { return true; }
    bool is_connected() const override { return connected; }
    bool is_valid() const override { return valid; }
    void reset_state() noexcept override {}
    ~DummyConnector() override = default;
};

namespace ConnectorFactory {
    std::unique_ptr<DatabaseConnector> create(const DataChannel&, const ConnectionInfo&) {
        return std::make_unique<DummyConnector>();
    }
}