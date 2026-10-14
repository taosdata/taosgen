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

#include "FilePathResolver.hpp"
#include "StringUtils.hpp"
#include "FilesystemCompat.hpp"
#include <algorithm>
#include <stdexcept>


bool FilePathResolver::is_directory(const std::string& path) {
    return fs::is_directory(path);
}

bool FilePathResolver::has_glob_pattern(const std::string& path) {
    return path.find('*') != std::string::npos || path.find('?') != std::string::npos;
}

bool FilePathResolver::glob_match(const std::string& pattern, const std::string& str) {
    size_t pi = 0, si = 0;
    size_t star_p = std::string::npos, star_s = 0;

    while (si < str.size()) {
        if (pi < pattern.size() && (pattern[pi] == '?' || pattern[pi] == str[si])) {
            ++pi;
            ++si;
        } else if (pi < pattern.size() && pattern[pi] == '*') {
            star_p = pi++;
            star_s = si;
        } else if (star_p != std::string::npos) {
            pi = star_p + 1;
            si = ++star_s;
        } else {
            return false;
        }
    }

    while (pi < pattern.size() && pattern[pi] == '*') {
        ++pi;
    }
    return pi == pattern.size();
}

std::vector<std::string> FilePathResolver::resolve_directory(const std::string& dir_path) {
    std::vector<std::string> results;
    for (const auto& entry : fs::directory_iterator(dir_path)) {
        if (fs::is_regular_file(entry.path())) {
            auto ext = StringUtils::to_lower(entry.path().extension().string());
            if (ext == ".csv") {
                results.push_back(entry.path().string());
            }
        }
    }
    std::sort(results.begin(), results.end());
    return results;
}

std::vector<std::string> FilePathResolver::resolve_glob(const std::string& pattern) {
    fs::path pat(pattern);
    fs::path parent = pat.parent_path();
    std::string filename_pattern = pat.filename().string();

    if (parent.empty()) {
        parent = ".";
    }

    if (!fs::is_directory(parent)) {
        throw std::runtime_error("Glob pattern parent directory does not exist: " + parent.string());
    }

    std::vector<std::string> results;
    for (const auto& entry : fs::directory_iterator(parent)) {
        if (fs::is_regular_file(entry.path())) {
            std::string name = entry.path().filename().string();
            if (glob_match(filename_pattern, name)) {
                results.push_back(entry.path().string());
            }
        }
    }
    std::sort(results.begin(), results.end());
    return results;
}

std::vector<std::string> FilePathResolver::resolve(const std::string& file_path) {
    if (file_path.empty()) {
        throw std::runtime_error("File path is empty");
    }

    std::vector<std::string> results;

    if (has_glob_pattern(file_path)) {
        results = resolve_glob(file_path);
    } else if (is_directory(file_path)) {
        results = resolve_directory(file_path);
    } else {
        // Single file — verify it exists
        if (!fs::exists(file_path)) {
            throw std::runtime_error("CSV file does not exist: " + file_path);
        }
        results.push_back(file_path);
    }

    if (results.empty()) {
        throw std::runtime_error("No CSV files found matching path: " + file_path);
    }

    return results;
}
