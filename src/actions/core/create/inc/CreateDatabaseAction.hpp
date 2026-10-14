#pragma once

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

#include <iostream>
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "CreateDatabaseConfig.hpp"
#include "DatabaseConnector.hpp"


class CreateDatabaseAction : public ActionBase {
public:
    explicit CreateDatabaseAction(const GlobalConfig& global, const CreateDatabaseConfig& config) : global_(global), config_(config) {}

    void execute() override;

private:
    const GlobalConfig& global_;
    CreateDatabaseConfig config_;

    std::unique_ptr<DatabaseConnector> connector_;

    void prepare_connector();

    // Register CreateDatabaseAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/create-database",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<CreateDatabaseAction>(global, std::get<CreateDatabaseConfig>(config));
            });
        return true;
    }();
};