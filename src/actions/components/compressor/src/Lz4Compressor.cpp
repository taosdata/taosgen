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

#include "Lz4Compressor.hpp"
#include <vector>
#include <cstring>
#include <lz4frame.h>

std::string Lz4Compressor::compress(const std::string& data) {
    LZ4F_preferences_t prefs = {};
    size_t max_dst_size = LZ4F_compressFrameBound(data.size(), &prefs);
    std::vector<char> compressed(max_dst_size);

    size_t compressed_size = LZ4F_compressFrame(
        compressed.data(), max_dst_size,
        data.data(), data.size(),
        &prefs
    );
    if (LZ4F_isError(compressed_size)) {
        throw std::runtime_error("LZ4F_compressFrame failed: " + std::string(LZ4F_getErrorName(compressed_size)));
    }
    return std::string(compressed.data(), compressed_size);
}

std::string Lz4Compressor::decompress(const std::string& data) {
    size_t dst_capacity = 4 * data.size();
    std::vector<char> decompressed(dst_capacity);

    LZ4F_decompressionContext_t dctx;
    size_t err = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    if (LZ4F_isError(err)) {
        throw std::runtime_error("LZ4F_createDecompressionContext failed");
    }

    size_t src_size = data.size();
    size_t dst_size = decompressed.size();
    size_t src_pos = 0, dst_pos = 0;

    while (src_pos < src_size) {
        size_t src_chunk = src_size - src_pos;
        size_t dst_chunk = dst_size - dst_pos;
        size_t ret = LZ4F_decompress(
            dctx,
            decompressed.data() + dst_pos, &dst_chunk,
            data.data() + src_pos, &src_chunk,
            nullptr
        );
        if (LZ4F_isError(ret)) {
            LZ4F_freeDecompressionContext(dctx);
            throw std::runtime_error("LZ4F_decompress failed: " + std::string(LZ4F_getErrorName(ret)));
        }
        src_pos += src_chunk;
        dst_pos += dst_chunk;
        if (ret == 0) break;
        if (dst_pos == dst_size) {
            decompressed.resize(dst_size * 2);
            dst_size = decompressed.size();
        }
    }
    LZ4F_freeDecompressionContext(dctx);
    return std::string(decompressed.data(), dst_pos);
}