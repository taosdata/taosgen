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

#include "ZstdCompressor.hpp"
#include <vector>

std::string ZstdCompressor::compress(const std::string& data) {
    size_t max_size = ZSTD_compressBound(data.size());
    if (ZSTD_isError(max_size)) {
        throw std::runtime_error("ZSTD_compressBound failed");
    }

    std::vector<char> compressed(max_size);
    size_t compressed_size = ZSTD_compress(
        compressed.data(), max_size,
        data.data(), data.size(),
        1  // Default compression level
    );

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error("ZSTD compression failed: " +
            std::string(ZSTD_getErrorName(compressed_size)));
    }

    return {compressed.data(), compressed_size};
}

std::string ZstdCompressor::decompress(const std::string& data) {
    unsigned long long uncompressed_size = ZSTD_getFrameContentSize(
        data.data(), data.size()
    );

    if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error("Invalid ZSTD frame");
    }
    if (uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error("Unknown content size");
    }

    std::string result(uncompressed_size, '\0');
    size_t actual_size = ZSTD_decompress(
        result.data(), uncompressed_size,
        data.data(), data.size()
    );

    if (ZSTD_isError(actual_size)) {
        throw std::runtime_error("ZSTD decompression failed: " +
            std::string(ZSTD_getErrorName(actual_size)));
    }

    return result;
}