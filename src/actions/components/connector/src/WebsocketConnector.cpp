#include "WebsocketConnector.hpp"
#include <iostream>


WebsocketConnector::WebsocketConnector(const ConnectionInfo& conn_info)
    : conn_info_(conn_info) {}

WebsocketConnector::~WebsocketConnector() {
    close();
}

bool WebsocketConnector::connect() {
    throw std::runtime_error("WebsocketConnector::connect is not implemented.");
}

bool WebsocketConnector::select_db(const std::string& db_name) {
    (void)db_name;
    throw std::runtime_error("WebsocketConnector::select_db is not implemented.");
}

bool WebsocketConnector::prepare(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("WebsocketConnector::prepare is not implemented.");
}

bool WebsocketConnector::execute(const std::string& sql) {
    (void)sql;
    throw std::runtime_error("WebsocketConnector::execute is not implemented.");
}

bool WebsocketConnector::execute(const SqlInsertData& data) {
    (void)data;
    throw std::runtime_error("WebsocketConnector::execute is not implemented.");
}

bool WebsocketConnector::execute(const StmtV2InsertData& data) {
    (void)data;
    throw std::runtime_error("WebsocketConnector::execute is not implemented.");
}

void WebsocketConnector::close() noexcept {
    std::cerr << "WebsocketConnector::close is not implemented." << std::endl;
    std::abort();
}