#include <iostream>
#include "ParameterContext.h"
#include "JobScheduler.h"


int main(int argc, char* argv[]) {
    try {
        // 1. Create parameter context and initialize
        ParameterContext context;

        // Initialize parameter context
        if (!context.init(argc, argv)) {
            return 1;
        }

        // 2. Get parsed configuration data
        const ConfigData& config = context.get_config_data();

        // 3. Create and run job scheduler
        try {
            // Create job scheduler instance
            JobScheduler scheduler(config);
            
            // Run scheduler
            scheduler.run();

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