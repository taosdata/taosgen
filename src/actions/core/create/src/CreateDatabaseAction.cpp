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

#include "CreateDatabaseAction.hpp"
#include "LogUtils.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include "CheckpointAction.hpp"
#include <iostream>

void CreateDatabaseAction::prepare_connector() {
    auto tdengine = config_.tdengine;
    tdengine.database.clear();
    connector_ = ConnectorFactory::create(
        tdengine
    );
}

void CreateDatabaseAction::execute() {
    LogUtils::info("Creating database: {}", config_.tdengine.database);

    try {
        prepare_connector();

        auto formatter = FormatterFactory::create_formatter<CreateDatabaseConfig>(DataFormat());

        if (CheckpointAction::is_recover(global_, config_.checkpoint_info)) {
            config_.tdengine.drop_if_exists = false;
            LogUtils::info("Checkpoint file exists. Skipping database drop");
        }

        FormatResult result = formatter->format(config_);
        const auto& stmts = std::get<std::vector<std::string>>(result);

        for (const auto& stmt : stmts) {
            if (!connector_->execute(stmt)) {
                throw std::runtime_error("Failed to execute SQL: " + stmt);
            }
        }
    } catch (const std::exception& e) {
        LogUtils::error("An error occurred: {}", e.what());
        throw;
    }

    connector_->close();
}
