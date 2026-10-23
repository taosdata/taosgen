/*
 * Copyright (c) 2026 TAOS Data, Inc.
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

#include "ParameterContext.hpp"
#include "PluginRegistrar.hpp"
#include "KafkaConfig.hpp"
#include "LogUtils.hpp"
#include "ScopedEnvVar.hpp"
#include <cassert>
#include <iostream>

// Test command line parameter parsing
void test_commandline_merge() {
    ParameterContext ctx;
    const char* argv[] = {
        "dummy_program",
        "--host=cli.host",
        "--port=1234",
        "--user=cli_user",
        "--password=cli_pass"
    };
    ctx.init(5, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "cli.host:1234");

    std::cout << "Commandline merge test passed.\n";
}

// Test environment variable merge
void test_environment_merge() {
    ParameterContext ctx;

    ScopedEnvVar host_guard("TAOS_HOST", "env.host");
    ScopedEnvVar port_guard("TAOS_PORT", "5678");

    const char* argv[] = {"dummy"};
    ctx.init(1, const_cast<char**>(argv));

    const auto& global = ctx.get_global_config();
    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "env.host:5678");

    std::cout << "Environment merge test passed.\n";
}

void test_kafka_global_config() {
    ParameterContext ctx;
    YAML::Node config = YAML::Load(R"(
kafka:
  bootstrap_servers: "kafka.broker:9092"
)");
    ctx.merge_yaml(config);
    const auto& global = ctx.get_global_config();
    const auto& kc = get_plugin_config<KafkaConfig>(global.extensions, "kafka");
    (void)kc;
    assert(kc != nullptr);
    assert(kc->bootstrap_servers == "kafka.broker:9092");

    std::cout << "Kafka global config test passed.\n";
}

int main() {
    register_plugin_hooks();
    test_commandline_merge();
    test_environment_merge();
    test_kafka_global_config();

    std::cout << "All tests passed!\n";
    return 0;
}
