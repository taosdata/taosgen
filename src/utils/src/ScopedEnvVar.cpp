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

#include "ScopedEnvVar.hpp"
#include <cstdlib>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

ScopedEnvVar::ScopedEnvVar(const std::string& name, const std::string& new_value)
    : name_(name), had_old_value_(false) {

    if (name_.empty()) {
        throw std::invalid_argument("ScopedEnvVar: environment variable name cannot be empty");
    }

    const char* old = getenv(name_.c_str());
    if (old != nullptr) {
        old_value_ = old;
        had_old_value_ = true;
    }

#ifdef _WIN32
    if (_putenv_s(name_.c_str(), new_value.c_str()) != 0) {
        throw std::runtime_error("ScopedEnvVar: _putenv_s failed for " + name_);
    }
#else
    if (setenv(name_.c_str(), new_value.c_str(), 1) != 0) {
        throw std::runtime_error("ScopedEnvVar: setenv failed for " + name_);
    }
#endif
}

ScopedEnvVar::~ScopedEnvVar() {
    restore();
}

void ScopedEnvVar::restore() {
#ifdef _WIN32
    if (had_old_value_) {
        if (_putenv_s(name_.c_str(), old_value_.c_str()) != 0) {
            throw std::runtime_error("ScopedEnvVar: _putenv_s failed (restore old) for " + name_);
        }
    } else {
        // Setting to empty string effectively removes it from CRT environment
        if (_putenv_s(name_.c_str(), "") != 0) {
            throw std::runtime_error("ScopedEnvVar: _putenv_s failed (unset) for " + name_);
        }
    }
#else
    if (had_old_value_) {
        if (setenv(name_.c_str(), old_value_.c_str(), 1) != 0) {
            throw std::runtime_error("ScopedEnvVar: setenv failed (restore old)");
        }
    } else {
        if (unsetenv(name_.c_str()) != 0) {
            throw std::runtime_error("ScopedEnvVar: unsetenv failed");
        }
    }
#endif
}
