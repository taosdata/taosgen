
#include "ConnectorFactory.hpp"
#include "NativeConnector.hpp"
#include "WebsocketConnector.hpp"
#include "RestfulConnector.hpp"
#include <stdexcept>

std::unique_ptr<DatabaseConnector> ConnectorFactory::create(
    const DataChannel& channel,
    const ConnectionInfo& conn_info)
{
    if (channel.channel_type == "native") {
        return std::make_unique<NativeConnector>(conn_info);
    } else if (channel.channel_type == "websocket") {
        return std::make_unique<WebsocketConnector>(conn_info);
    } else if (channel.channel_type == "restful") {
        return std::make_unique<RestfulConnector>(conn_info);
    }

    throw std::invalid_argument("Unsupported channel type: " + channel.channel_type);
}