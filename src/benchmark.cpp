#include <iostream>
#include "ParameterContext.hpp"
#include "JobScheduler.hpp"
#include <csignal>
#include "actions/core/checkpoint/inc/CheckpointAction.hpp"


void signal_handler(int signum) {
    std::cout << "\nInterrupt signal (" << signum << ") received. Shutting down gracefully..." << std::endl;
    CheckpointAction::stop_all(true);
    exit(signum);
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    try {
        // 1. Create parameter context and initialize
        ParameterContext context;

        // Initialize parameter context
        if (!context.init(argc, argv)) {
            return 0;
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

            std::cout << "All jobs completed successfully!" << std::endl;
            return 0;

        } catch (const std::exception& e) {
            std::cerr << "Error during job execution: " << e.what() << std::endl;
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::cerr << "Use --help or -? to show usage information." << std::endl;
        return 1;
    }
}