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
#include <cassert>
#include <iostream>
#include <string>

void test_none_compressor() {
    std::string data = "hello world";
    std::string compressed = Compressor::compress(data, CompressionType::NONE);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::NONE);
    assert(compressed == data);
    assert(decompressed == data);
    std::cout << "test_none_compressor passed.\n";
}

void test_gzip_compressor() {
    std::string data = "gzip test data 1234567890";
    std::string compressed = Compressor::compress(data, CompressionType::GZIP);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::GZIP);
    assert(decompressed == data);
    std::cout << "test_gzip_compressor passed.\n";
}

void test_lz4_compressor() {
    std::string data = "lz4 test data abcdefghijklmnopqrstuvwxyz";
    std::string compressed = Compressor::compress(data, CompressionType::LZ4);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::LZ4);
    assert(decompressed == data);
    std::cout << "test_lz4_compressor passed.\n";
}

void test_zstd_compressor() {
    std::string data = "zstd test data 0987654321";
    std::string compressed = Compressor::compress(data, CompressionType::ZSTD);
    assert(compressed != data);
    std::string decompressed = Compressor::decompress(compressed, CompressionType::ZSTD);
    assert(decompressed == data);
    std::cout << "test_zstd_compressor passed.\n";
}

void test_invalid_type() {
    std::string data = "invalid type";
    bool caught = false;
    try {
        Compressor::compress(data, static_cast<CompressionType>(999));
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("Unsupported compression type") != std::string::npos);
        caught = true;
    }
    (void)caught;
    assert(caught);
    std::cout << "test_invalid_type passed.\n";
}

int main() {
    test_none_compressor();
    test_gzip_compressor();
    test_lz4_compressor();
    test_zstd_compressor();
    test_invalid_type();
    std::cout << "All Compressor tests passed.\n";
    return 0;
}