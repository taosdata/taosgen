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
#include <fstream>
#include <yaml-cpp/yaml.h>


int main() {
    // Open YAML file
    std::cout << "Opening config.yaml: " << CONFIG_PATH << std::endl;
    std::ifstream file(CONFIG_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open config.yaml" << std::endl;
        return 1;
    }

    // Parse YAML file
    YAML::Node config;
    try {
        config = YAML::Load(file);
    } catch (const YAML::ParserException& e) {
        std::cerr << "YAML parse error: " << e.what() << std::endl;
        return 1;
    }

    // Output parsed result
    std::cout << "Parsed YAML content:" << std::endl;
    std::cout << config << std::endl;

    // Example: access YAML data
    if (config["global"]["host"]) {
        std::cout << "Host: " << config["host"].as<std::string>() << std::endl;
    }

    std::cout << "test_parse_yaml passed\n";

    return 0;
}