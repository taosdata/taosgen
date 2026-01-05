#include "PluginConfigRegistrar.hpp"

PluginConfigRegistrar& PluginConfigRegistrar::instance() {
    static PluginConfigRegistrar instance;
    return instance;
}

void PluginConfigRegistrar::register_parser(const std::string& plugin, Parser parser) {
    auto& inst = instance();
    inst.yaml_parsers_[plugin] = std::move(parser);
}

void PluginConfigRegistrar::parse_into_extensions(const YAML::Node& node, PluginExtensions& extensions, bool present_only) {
    auto& inst = instance();
    const auto& parsers = inst.yaml_parsers_;

    for (const auto& [key, parser] : parsers) {
        if (present_only && !node[key]) {
            continue;
        }

        extensions[key] = parser(node[key]);
    }
}

void PluginConfigRegistrar::register_cli_merger(const std::string& plugin, CliMerger merger) {
    auto& inst = instance();
    inst.cli_mergers_[plugin] = std::move(merger);
}

void PluginConfigRegistrar::register_env_merger(const std::string& plugin, EnvMerger merger) {
    auto& inst = instance();
    inst.env_mergers_[plugin] = std::move(merger);
}

void PluginConfigRegistrar::apply_cli_mergers(const std::unordered_map<std::string, std::string>& cli_params,
                                              PluginExtensions& extensions) {
    auto& inst = instance();
    for (const auto& [_, merger] : inst.cli_mergers_) {
        merger(cli_params, extensions);
    }
}

void PluginConfigRegistrar::apply_env_mergers(PluginExtensions& extensions) {
    auto& inst = instance();
    for (const auto& [_, merger] : inst.env_mergers_) {
        merger(extensions);
    }
}

void PluginConfigRegistrar::register_format_decoder(const std::string& target, FormatDecoder decoder) {
    auto& inst = instance();
    inst.format_decoders_[target] = std::move(decoder);
}

bool PluginConfigRegistrar::apply_format_decoder(const std::string& target, const YAML::Node& node, InsertDataConfig& cfg) {
    auto& inst = instance();
    auto it = inst.format_decoders_.find(target);
    if (it == inst.format_decoders_.end()) return false;
    it->second(node, cfg);
    return true;
}