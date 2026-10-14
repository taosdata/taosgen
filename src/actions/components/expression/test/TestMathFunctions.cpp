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

#include "MathFunctions.hpp"
#include <cassert>
#include <iostream>

void test_square_wave_basic() {
    // period = 4, offset = 0
    double min = 1.0, max = 5.0;
    int period = 4, offset = 0;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(1, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(2, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(3, min, max, period, offset) == min);
    std::cout << "test_square_wave_basic passed.\n";
}

void test_square_wave_offset() {
    double min = 2.0, max = 8.0;
    int period = 4, offset = 1;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(1, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(2, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(3, min, max, period, offset) == max);
    std::cout << "test_square_wave_offset passed.\n";
}

void test_square_wave_invalid_period() {
    double min = 3.0, max = 7.0;
    int period = 0, offset = 0;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == min);
    std::cout << "test_square_wave_invalid_period passed.\n";
}

int main() {
    test_square_wave_basic();
    test_square_wave_offset();
    test_square_wave_invalid_period();
    std::cout << "All MathFunctions tests passed.\n";
    return 0;
}