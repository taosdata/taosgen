#pragma once

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

#include <string>
#include <optional>
#include "ColumnConfig.hpp"


class ColumnConfigInstance {
public:
    ColumnConfigInstance(const ColumnConfig& config, size_t index = 1) : config_(config) {
        config_.parse_type();

        if (config_.count > 1) {
            name_ = config_.name + std::to_string(index);
        } else {
            name_ = config_.name;
        }
    }

    const std::string& name() const {
        return name_;
    }

    const std::string& type() const {
        return config_.type;
    }

    ColumnTypeTag type_tag() const {
        return config_.type_tag;
    }    

    const std::optional<std::string>& gen_type() const {
        return config_.gen_type;
    }

    const ColumnConfig& config() const {
        return config_;
    }

private:
    std::string name_;
    ColumnConfig config_;
};

using ColumnConfigInstanceVector = std::vector<ColumnConfigInstance>;

class ColumnConfigInstanceFactory {
public:
    static ColumnConfigInstanceVector create(const ColumnConfig& config) {
        ColumnConfigInstanceVector instances;
        instances.reserve(config.count);
        for (size_t i = 1; i <= config.count; ++i) {
            instances.emplace_back(config, i);
        }
        return instances;
    }

    static ColumnConfigInstanceVector create(const ColumnConfigVector& configs) {
        ColumnConfigInstanceVector instances;
        for (const auto& config : configs) {
            auto config_instances = create(config);
            instances.insert(instances.end(), config_instances.begin(), config_instances.end());
        }
        return instances;
    }

};
