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

#include "CompressionType.hpp"
#include "StringUtils.hpp"
#include <stdexcept>

const char* compression_to_string(CompressionType type) {
    switch (type) {
        case CompressionType::NONE: return "NONE";
        case CompressionType::GZIP: return "GZIP";
        case CompressionType::LZ4:  return "LZ4";
        case CompressionType::ZSTD: return "ZSTD";
        default: return "UNKNOWN";
    }
}

CompressionType string_to_compression(const std::string& str) {
    std::string s = StringUtils::to_upper(str);
    if (s == "" || s == "NONE")  return CompressionType::NONE;
    if (s == "GZIP")  return CompressionType::GZIP;
    if (s == "LZ4")   return CompressionType::LZ4;
    if (s == "ZSTD")  return CompressionType::ZSTD;
    throw std::invalid_argument("Unknown compression type: " + str);
}