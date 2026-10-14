
/*
 * Copyright (c) 2025 TAOS Data, Inc.
 *
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "ConnectorFactory.hpp"
#include "NativeConnector.hpp"
#include "WebsocketConnector.hpp"
#include "RestfulConnector.hpp"
#include <stdexcept>

std::unique_ptr<DatabaseConnector> ConnectorFactory::create(const TDengineConfig& conn_info) {
    if (conn_info.protocol_type == TDengineConfig::ProtocolType::Native) {
        return std::make_unique<NativeConnector>(conn_info);
    } else if (conn_info.protocol_type == TDengineConfig::ProtocolType::WebSocket) {
        return std::make_unique<WebsocketConnector>(conn_info);
    } else if (conn_info.protocol_type == TDengineConfig::ProtocolType::RESTful) {
        return std::make_unique<RestfulConnector>(conn_info);
    }

    throw std::invalid_argument("Unsupported channel type: " + std::to_string(static_cast<int>(conn_info.protocol_type)));
}