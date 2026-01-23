#include "LogUtils.hpp"
#include "SignalManager.hpp"
#include "ParameterContext.hpp"
#include "JobScheduler.hpp"
#include "PluginRegistrar.hpp"
#include <iostream>
#include <chrono>
#include <thread>

void exit_handler(int signum) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    LogUtils::info("Interrupt signal ({}) received. Shutting down gracefully...", signum);
    exit(signum);
}

int main(int argc, char* argv[]) {
    LogUtils::LoggerGuard logger_guard(LogUtils::Level::Info);

    SignalManager::register_signal(SIGINT, exit_handler, true);
    SignalManager::register_signal(SIGTERM, exit_handler, true);
    SignalManager::setup();

    try {
        // 1. Create parameter context and initialize
        ParameterContext context;

        register_plugin_hooks();

        // Initialize parameter context
        if (!context.init(argc, argv)) {
            return 0;
        }

        if (context.get_global_config().verbose) {
            logger_guard.set_level(LogUtils::Level::Debug);
        }

        // 2. Get parsed configuration data
        const ConfigData& config = context.get_config_data();

        // 3. Create and run job scheduler
        try {
            // Create job scheduler instance
            JobScheduler scheduler(config);

            // Run scheduler
            bool success = scheduler.run();
            if (!success) {
                return 1;
            }

            LogUtils::info("All jobs completed successfully!");
            return 0;

        } catch (const std::exception& e) {
            LogUtils::error("Error during job execution: {}", e.what());
            return 1;
        }

    } catch (const std::exception& e) {
        LogUtils::error(e.what());
        LogUtils::info("Use --help or -? to show usage information");
        return 1;
    }
}