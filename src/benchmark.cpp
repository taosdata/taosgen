#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include "ParameterContext.hpp"
#include "JobScheduler.hpp"
#include "PluginRegistrar.hpp"
#include <iostream>
#include <chrono>
#include <thread>

static JobScheduler* g_scheduler = nullptr;

void exit_handler(int signum) {
    (void)signum;
    if (g_scheduler) {
        g_scheduler->stop();
    }
}

int main(int argc, char* argv[]) {
    LogUtils::LoggerGuard logger_guard(LogUtils::Level::Info, "");

    SignalManager::register_signal(SIGINT, exit_handler, true);
    SignalManager::register_signal(SIGTERM, exit_handler, true);
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

        try {
            // Create job scheduler instance
            JobScheduler scheduler(config);
            g_scheduler = &scheduler;

            // Run scheduler
            bool success = scheduler.run();
            g_scheduler = nullptr;

            if (!success) {
                if (scheduler.was_interrupted()) {
                    LogUtils::info("Interrupt signal received. Shutting down gracefully...");
                }
                return 1;
            }

            LogUtils::info("All jobs completed successfully!");
            return 0;

        } catch (const std::exception& e) {
            g_scheduler = nullptr;
            LogUtils::error("Error during job execution: {}", e.what());
            return 1;
        }

    } catch (const std::exception& e) {
        LogUtils::error(e.what());
        LogUtils::info("Use --help or -? to show usage information");
        return 1;
    }
}