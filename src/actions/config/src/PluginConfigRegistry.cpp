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

#include "PluginConfigRegistry.hpp"

PluginConfigRegistry& PluginConfigRegistry::instance() {
    static PluginConfigRegistry instance;
    return instance;
}

void PluginConfigRegistry::register_parser(const std::string& plugin, Parser parser) {
    auto& inst = instance();
    inst.yaml_parsers_[plugin] = std::move(parser);
}

void PluginConfigRegistry::parse_into_extensions(const YAML::Node& node, PluginExtensions& extensions, bool present_only) {
    auto& inst = instance();
    const auto& parsers = inst.yaml_parsers_;

    for (const auto& [key, parser] : parsers) {
        if (present_only && !node[key]) {
            continue;
        }

        extensions[key] = parser(node[key]);
    }
}

void PluginConfigRegistry::register_cli_merger(const std::string& plugin, CliMerger merger) {
    auto& inst = instance();
    inst.cli_mergers_[plugin] = std::move(merger);
}

void PluginConfigRegistry::register_env_merger(const std::string& plugin, EnvMerger merger) {
    auto& inst = instance();
    inst.env_mergers_[plugin] = std::move(merger);
}

void PluginConfigRegistry::apply_cli_mergers(const std::unordered_map<std::string, std::string>& cli_params,
                                              PluginExtensions& extensions) {
    auto& inst = instance();
    for (const auto& kv : inst.cli_mergers_) {
        kv.second(cli_params, extensions);
    }
}

void PluginConfigRegistry::apply_env_mergers(PluginExtensions& extensions) {
    auto& inst = instance();
    for (const auto& kv : inst.env_mergers_) {
        kv.second(extensions);
    }
}

void PluginConfigRegistry::register_format_decoder(const std::string& target, FormatDecoder decoder) {
    auto& inst = instance();
    inst.format_decoders_[target] = std::move(decoder);
}

bool PluginConfigRegistry::apply_format_decoder(const std::string& target, const YAML::Node& node, InsertDataConfig& cfg) {
    auto& inst = instance();
    auto it = inst.format_decoders_.find(target);
    if (it == inst.format_decoders_.end()) return false;
    it->second(node, cfg);
    return true;
}