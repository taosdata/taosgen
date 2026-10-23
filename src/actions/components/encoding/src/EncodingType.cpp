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

#include "EncodingType.hpp"
#include "StringUtils.hpp"
#include <algorithm>
#include <string>
#include <stdexcept>

const char* encoding_to_string(EncodingType type) {
    switch (type) {
        case EncodingType::NONE:     return "NONE";
        case EncodingType::GBK:      return "GBK";
        case EncodingType::GB18030:  return "GB18030";
        case EncodingType::BIG5:     return "BIG5";
        case EncodingType::UTF8:     return "UTF-8";
        default:                     return "UNKNOWN";
    }
}

EncodingType string_to_encoding(const std::string& str) {
    std::string s = StringUtils::to_upper(str);
    if (s == "" || s == "NONE")         return EncodingType::UTF8; // NONE defaults to UTF-8
    if (s == "GBK")                     return EncodingType::GBK;
    if (s == "GB18030")                 return EncodingType::GB18030;
    if (s == "BIG5")                    return EncodingType::BIG5;
    if (s == "UTF-8" || s == "UTF8")    return EncodingType::UTF8;
    throw std::invalid_argument("Unknown encoding type: " + str);
}