#pragma once

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

#include "PluginExtensions.hpp"
#include <any>
#include <functional>
#include <string>
#include <unordered_map>
#include <yaml-cpp/yaml.h>

struct InsertDataConfig;

class PluginConfigRegistry {
public:
    using Parser        = std::function<std::any(const YAML::Node&)>;
    using CliMerger     = std::function<void(const std::unordered_map<std::string, std::string>&, PluginExtensions&)>;
    using EnvMerger     = std::function<void(PluginExtensions&)>;
    using FormatDecoder = std::function<void(const YAML::Node&, InsertDataConfig&)>;

    static PluginConfigRegistry& instance();

    // YAML Parsing Registration and Dispatching
    static void register_parser(const std::string& plugin, Parser parser);
    static void parse_into_extensions(const YAML::Node& node, PluginExtensions& extensions, bool present_only = true);

    // CLI/ENV Merge Registration and Dispatching
    static void register_cli_merger(const std::string& plugin, CliMerger merger);
    static void apply_cli_mergers(const std::unordered_map<std::string, std::string>& cli_params, PluginExtensions& extensions);

    static void register_env_merger(const std::string& plugin, EnvMerger merger);
    static void apply_env_mergers(PluginExtensions& extensions);

    // Plugin formatting configuration decoding Registration and Dispatching
    static void register_format_decoder(const std::string& target, FormatDecoder decoder);
    static bool apply_format_decoder(const std::string& target, const YAML::Node& node, InsertDataConfig& cfg);

private:
    std::unordered_map<std::string, Parser>        yaml_parsers_;
    std::unordered_map<std::string, CliMerger>     cli_mergers_;
    std::unordered_map<std::string, EnvMerger>     env_mergers_;
    std::unordered_map<std::string, FormatDecoder> format_decoders_;
};