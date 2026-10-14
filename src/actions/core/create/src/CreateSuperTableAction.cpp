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

#include "CreateSuperTableAction.hpp"
#include "LogUtils.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include <iostream>

void CreateSuperTableAction::prepare_connector() {
    connector_ = ConnectorFactory::create(
        config_.tdengine
    );
}

void CreateSuperTableAction::execute() {
    LogUtils::info("Creating super table: {}.{}", config_.tdengine.database, config_.schema.name);

    try {
        prepare_connector();

        auto formatter = FormatterFactory::create_formatter<CreateSuperTableConfig>(DataFormat());
        FormatResult formatted_result = formatter->format(config_);
        auto sql = std::get<std::string>(formatted_result);
        if (!connector_->execute(sql)) {
            throw std::runtime_error("Failed to execute SQL: " + sql);
        }

    } catch (const std::exception& e) {
        LogUtils::error("An error occurred: {}", e.what());
        throw;
    }

    connector_->close();
}
