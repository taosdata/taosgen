
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

#include "ColumnGeneratorFactory.hpp"
#include <stdexcept>
#include "RandomColumnGenerator.hpp"
#include "OrderColumnGenerator.hpp"
#include "ExprColumnGenerator.hpp"


std::unique_ptr<ColumnGenerator> ColumnGeneratorFactory::create(const std::string& table_name, const ColumnConfigInstance& instance) {
    if (!instance.config().gen_type) return nullptr;

    const std::string& gen_type = *instance.config().gen_type;

    if (gen_type == "random") {
        return std::make_unique<RandomColumnGenerator>(instance);
    }
    else if (gen_type == "order") {
        return std::make_unique<OrderColumnGenerator>(instance);
    }
    else if (gen_type == "expression") {
        return std::make_unique<ExprColumnGenerator>(table_name, instance);
    }

    throw std::runtime_error("Unsupported generator type: " + gen_type);
}