#include <iostream>
#include <fstream>
#include <yaml-cpp/yaml.h>


int main() {
    // 打开 YAML 文件
    std::cout << "Opening config.yaml: " << CONFIG_PATH << std::endl;
    std::ifstream file(CONFIG_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open config.yaml" << std::endl;
        return 1;
    }

    // 解析 YAML 文件
    YAML::Node config;
    try {
        config = YAML::Load(file);
    } catch (const YAML::ParserException& e) {
        std::cerr << "YAML parse error: " << e.what() << std::endl;
        return 1;
    }

    // 输出解析结果
    std::cout << "Parsed YAML content:" << std::endl;
    std::cout << config << std::endl;

    // 示例：访问 YAML 数据
    if (config["global"]["host"]) {
        std::cout << "Host: " << config["host"].as<std::string>() << std::endl;
    }

    std::cout << "test_parse_yaml passed\n";

    return 0;
}