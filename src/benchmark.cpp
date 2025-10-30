#include "LogUtils.hpp"
#include "ParameterContext.hpp"
#include "JobScheduler.hpp"
#include <iostream>
#include <csignal>
#include "actions/core/checkpoint/inc/CheckpointAction.hpp"

void signal_handler(int signum) {
    LogUtils::info("Interrupt signal (" + std::to_string(signum) + ") received. Shutting down gracefully...");
    CheckpointAction::stop_all(true);
    exit(signum);
}

int main(int argc, char* argv[]) {
    int result = 0;

    LogUtils::init(LogUtils::Level::Info);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    try {
        // 1. Create parameter context and initialize
        ParameterContext context;

        // Initialize parameter context
        if (!context.init(argc, argv)) {
            goto end;
        }

        if (context.get_global_config().verbose) {
            LogUtils::set_level(LogUtils::Level::Debug);
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
                result = 1;
                goto end;
            }

            LogUtils::info("All jobs completed successfully!");
            goto end;

        } catch (const std::exception& e) {
            LogUtils::error("Error during job execution: " + std::string(e.what()));
            result = 1;
            goto end;
        }

    } catch (const std::exception& e) {
        LogUtils::error("Error: " + std::string(e.what()));
        LogUtils::error("Use --help or -? to show usage information");
        result = 1;
        goto end;
    }

end:
    LogUtils::shutdown();
    return result;
}