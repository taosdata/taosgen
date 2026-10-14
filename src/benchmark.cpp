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

#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include "ParameterContext.hpp"
#include "JobScheduler.hpp"
#include "PluginRegistrar.hpp"
#include "IngestionServer.hpp"
#include "IngestionTDengineWriter.hpp"
#include <iostream>
#include <chrono>
#include <thread>

namespace {

bool run_configured_jobs(const ConfigData& config, bool startup_jobs) {
    if (config.jobs.empty()) {
        return true;
    }

    try {
        JobScheduler scheduler(config);
        bool success = scheduler.run();

        if (!success) {
            if (scheduler.was_interrupted()) {
                LogUtils::info("Interrupt signal received. Shutting down gracefully...");
            }
            return false;
        }

        LogUtils::info(startup_jobs ? "All startup jobs completed successfully!" : "All jobs completed successfully!");
        return true;
    } catch (const std::exception& e) {
        LogUtils::error("Error during job execution: {}", e.what());
        return false;
    }
}

int run_ingress_server(ParameterContext& context) {
    const auto& ingress = context.get_ingress_config();
    LogUtils::info(
        "Starting ingress gRPC server: listen={}, service_concurrency={}, concurrency={}, default_ack_mode={}",
        ingress.listen,
        ingress.service_concurrency,
        ingress.concurrency,
        ack_mode_to_string(ingress.default_ack_mode)
    );

    auto writer = create_tdengine_ingestion_writer(context.get_config_data().global, ingress);
    IngestionServer server(context.get_config_data().global.schema, ingress, std::move(writer));
    server.start();
    LogUtils::info("Ingress gRPC server is listening on {}", server.listen_address());
    server.wait_until_interrupted();
    LogUtils::info("Ingress gRPC server stopped.");
    return 0;
}

} // namespace

int main(int argc, char* argv[]) {
    LogUtils::LoggerGuard logger_guard(LogUtils::Level::Info, "");

    // Register signals to install our handler (sets SignalManager::interrupted())
    SignalManager::register_signal(SIGINT);
    SignalManager::register_signal(SIGTERM);
    SignalManager::setup();

    try {
        // 1. Create parameter context and initialize
        ParameterContext context;

        register_plugin_hooks();

        if (!context.init_global(argc, argv)) {
            return 0;
        }

        // 2. Now that global config is merged, initialize full logging (console + file)
        std::string log_file = context.get_log_file_path();
        LogUtils::Level log_level = context.get_global_config().verbose ?
                                     LogUtils::Level::Debug : LogUtils::Level::Info;

        try {
            LogUtils::init(log_level, log_file);
            LogUtils::info("Log file initialized: {}", log_file);
        } catch (const std::exception& e) {
            LogUtils::error("Failed to initialize log file: {}", e.what());
            return 1;
        }

        // 3. Parse jobs
        context.init_jobs();

        // 4. Get parsed configuration data and run jobs
        const ConfigData& config = context.get_config_data();
        if (config.jobs.empty() && !context.is_ingress_mode()) {
            LogUtils::error("No jobs configured and ingress.enabled is not true; nothing to execute.");
            return 1;
        }

        if (!run_configured_jobs(config, context.is_ingress_mode())) {
            return 1;
        }

        if (context.is_ingress_mode()) {
            return run_ingress_server(context);
        }

        return 0;

    } catch (const std::exception& e) {
        LogUtils::error(e.what());
        LogUtils::info("Use --help or -? to show usage information");
        return 1;
    }
}
