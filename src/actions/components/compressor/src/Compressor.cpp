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

#include "Compressor.hpp"
#include "GzipCompressor.hpp"
#include "Lz4Compressor.hpp"
#include "ZstdCompressor.hpp"

class NoneCompressor : public BaseCompressor {
public:
    std::string compress(const std::string& data) override { return data; }
    std::string decompress(const std::string& data) override { return data; }
};

class GzipCompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return GzipCompressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return GzipCompressor::decompress(data);
    }
};

class Lz4CompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return Lz4Compressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return Lz4Compressor::decompress(data);
    }
};

class ZstdCompressorAdapter : public BaseCompressor {
public:
    std::string compress(const std::string& data) override {
        return ZstdCompressor::compress(data);
    }
    std::string decompress(const std::string& data) override {
        return ZstdCompressor::decompress(data);
    }
};

std::unique_ptr<BaseCompressor> Compressor::get_compressor(CompressionType type) {
    switch (type) {
        case CompressionType::NONE: return std::make_unique<NoneCompressor>();
        case CompressionType::GZIP: return std::make_unique<GzipCompressorAdapter>();
        case CompressionType::LZ4:  return std::make_unique<Lz4CompressorAdapter>();
        case CompressionType::ZSTD: return std::make_unique<ZstdCompressorAdapter>();
        default: throw std::runtime_error("Unsupported compression type");
    }
}

std::string Compressor::compress(const std::string& data, CompressionType type) {
    return get_compressor(type)->compress(data);
}

std::string Compressor::decompress(const std::string& data, CompressionType type) {
    return get_compressor(type)->decompress(data);
}