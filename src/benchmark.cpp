#include <iostream>
#include "ParameterContext.h"
#include "JobScheduler.h"


int main(int argc, char* argv[]) {
    try {
        // 1. 创建参数上下文并初始化
        ParameterContext context;

        // 初始化参数上下文
        if (!context.init(argc, argv)) {
            return 1;
        }

        // 2. 获取解析后的配置数据
        const ConfigData& config = context.get_config_data();

        // 3. 创建并运行作业调度器
        try {
            // 创建作业调度器实例
            JobScheduler scheduler(config);
            
            // 运行调度器
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
