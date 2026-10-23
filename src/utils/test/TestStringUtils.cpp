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

#include "StringUtils.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <string_view>

void test_trim_view_trims_leading_and_trailing_whitespace() {
    using namespace std::literals;
    assert(StringUtils::trim_view("  value\t\n"sv) == "value"sv);
    assert(StringUtils::trim_view("\r\n  padded text  \f"sv) == "padded text"sv);
}

void test_trim_view_returns_empty_for_whitespace_only() {
    using namespace std::literals;
    assert(StringUtils::trim_view("   \t\n\r"sv) == ""sv);
    assert(StringUtils::trim_view(""sv) == ""sv);
}

void test_iequals_ascii_matches_case_insensitively() {
    assert(StringUtils::iequals_ascii("NULL", "null"));
    assert(StringUtils::iequals_ascii("n/A", "n/a"));
    assert(StringUtils::iequals_ascii("Na", "na"));
}

void test_iequals_ascii_rejects_different_strings() {
    assert(!StringUtils::iequals_ascii("nulls", "null"));
    assert(!StringUtils::iequals_ascii("na", "null"));
    assert(!StringUtils::iequals_ascii("n-a", "n/a"));
}

void test_to_lower_basic() {
    assert(StringUtils::to_lower("HELLO") == "hello");
    assert(StringUtils::to_lower("Hello World") == "hello world");
    assert(StringUtils::to_lower("already lower") == "already lower");
    assert(StringUtils::to_lower("") == "");
    assert(StringUtils::to_lower("123ABC!@#") == "123abc!@#");
}

void test_to_lower_ascii_only() {
    // Non-ASCII bytes should pass through unchanged (no locale dependency)
    std::string input = "A\xC0\xC1Z";
    std::string result = StringUtils::to_lower(input);
    assert(result[0] == 'a');
    assert(result[1] == '\xC0');
    assert(result[2] == '\xC1');
    assert(result[3] == 'z');
}

void test_trim_view_ascii_only_whitespace() {
    std::string input = std::string("\xC2\xA0") + "value" + "\xC2\xA0";
    std::string_view view = StringUtils::trim_view(input);
    (void)view;
    assert(view.size() == input.size());
    assert(view == std::string_view(input));
}

void test_to_upper_basic() {
    assert(StringUtils::to_upper("hello") == "HELLO");
    assert(StringUtils::to_upper("Hello World") == "HELLO WORLD");
    assert(StringUtils::to_upper("ALREADY UPPER") == "ALREADY UPPER");
    assert(StringUtils::to_upper("") == "");
    assert(StringUtils::to_upper("123abc!@#") == "123ABC!@#");
}

void test_to_upper_ascii_only() {
    // Non-ASCII bytes should pass through unchanged (no locale dependency)
    std::string input = "a\xC0\xE1z";
    std::string result = StringUtils::to_upper(input);
    assert(result[0] == 'A');
    assert(result[1] == '\xC0');
    assert(result[2] == '\xE1');
    assert(result[3] == 'Z');
}

int main() {
    test_trim_view_trims_leading_and_trailing_whitespace();
    test_trim_view_returns_empty_for_whitespace_only();
    test_iequals_ascii_matches_case_insensitively();
    test_iequals_ascii_rejects_different_strings();
    test_to_lower_basic();
    test_to_lower_ascii_only();
    test_to_upper_basic();
    test_to_upper_ascii_only();
    test_trim_view_ascii_only_whitespace();

    std::cout << "TestStringUtils passed" << std::endl;
    return 0;
}
