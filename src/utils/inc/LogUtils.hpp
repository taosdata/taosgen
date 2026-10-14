#pragma once

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

#include <spdlog/fmt/fmt.h>
#include <spdlog/logger.h>
#include <spdlog/fmt/ostr.h>
#include <iostream>
#include <string>
#include <memory>

namespace LogUtils {

enum class Level {
    Debug,
    Info,
    Warn,
    Error,
    Fatal
};

// Initialize the log system with console only
void init_console(Level level = Level::Info);

// Initialize the log system with both console and file output
void init(Level level = Level::Info,
          const std::string& log_file = "log/taosgen.log",
          size_t max_file_size = 1024 * 1024 * 5,
          size_t max_files = 3);

void shutdown();
void set_level(Level level);
void flush(); // Flush all pending log messages

// Logger instance
extern std::shared_ptr<spdlog::logger> logger;

// String version
void debug(const std::string& msg);
void info(const std::string& msg);
void warn(const std::string& msg);
void error(const std::string& msg);
void fatal(const std::string& msg);

// Variadic template version (fmt-style)
template <typename... Args>
inline void debug(fmt::format_string<Args...> fmt, Args&&... args) {
    if (logger) {
        logger->debug(fmt, std::forward<Args>(args)...);
    } else {
        std::cout << "[DEBUG] " << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
    }
}

template <typename... Args>
inline void info(fmt::format_string<Args...> fmt, Args&&... args) {
    if (logger) {
        logger->info(fmt, std::forward<Args>(args)...);
    } else {
        std::cout << "[INFO] " << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
    }
}

template <typename... Args>
inline void warn(fmt::format_string<Args...> fmt, Args&&... args) {
    if (logger) {
        logger->warn(fmt, std::forward<Args>(args)...);
    } else {
        std::cout << "[WARN] " << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
    }
}

template <typename... Args>
inline void error(fmt::format_string<Args...> fmt, Args&&... args) {
    if (logger) {
        logger->error(fmt, std::forward<Args>(args)...);
    } else {
        std::cerr << "[ERROR] " << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
    }
}

template <typename... Args>
inline void fatal(fmt::format_string<Args...> fmt, Args&&... args) {
    if (logger) {
        logger->critical(fmt, std::forward<Args>(args)...);
    } else {
        std::cerr << "[FATAL] " << fmt::format(fmt, std::forward<Args>(args)...) << std::endl;
    }
}

class LoggerGuard {
public:
    LoggerGuard(Level level = Level::Info,
                const std::string& log_file = "log/taosgen.log",
                size_t max_file_size = 1024 * 1024 * 5,
                size_t max_files = 3) {
        if (log_file.empty()) {
            // Console-only mode
            LogUtils::init_console(level);
        } else {
            // Full logging mode (console + file)
            LogUtils::init(level, log_file, max_file_size, max_files);
        }
    }

    ~LoggerGuard() {
        LogUtils::shutdown();
    }

    void set_level(Level level) {
        LogUtils::set_level(level);
    }
};

}