#pragma once

#include "TDengineConnector.hpp"

class WebsocketConnector final : public TDengineConnector {
public:
    explicit WebsocketConnector(const ConnectionInfo& conn_info)
        : TDengineConnector(conn_info, "websocket", "WebSocket") {}
};