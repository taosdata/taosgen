#include "LogUtils.hpp"
#include <cassert>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>

bool log_file_contains(const std::string& log_file, const std::string& keyword) {
    std::ifstream fin(log_file);
    if (!fin.is_open()) return false;
    std::string line;
    while (std::getline(fin, line)) {
        if (line.find(keyword) != std::string::npos) return true;
    }
    return false;
}

void test_init_and_info_log() {
    std::string log_file = "testlog/test_info.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Hello Info Log");
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "Hello Info Log"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_init_and_info_log passed" << std::endl;
}

void test_debug_level_no_output() {
    std::string log_file = "testlog/test_debug.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug message should not appear");
    LogUtils::info("Info message should appear");
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(!log_file_contains(log_file, "Debug message should not appear"));
    assert(log_file_contains(log_file, "Info message should appear"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_debug_level_no_output passed" << std::endl;
}

void test_warn_error_fatal_log() {
    std::string log_file = "testlog/test_warn_error_fatal.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);
    LogUtils::init(LogUtils::Level::Debug, log_file, 1024 * 1024, 1);
    LogUtils::warn("Warn log");
    LogUtils::error("Error log");
    LogUtils::fatal("Fatal log");
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "Warn log"));
    assert(log_file_contains(log_file, "Error log"));
    assert(log_file_contains(log_file, "Fatal log"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_warn_error_fatal_log passed" << std::endl;
}

void test_log_file_directory_created() {
    std::string log_file = "testlog/subdir/test_create_dir.log";
    std::filesystem::path dir = std::filesystem::path(log_file).parent_path();
    if (std::filesystem::exists(dir)) std::filesystem::remove_all(dir);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Check directory creation");
    LogUtils::shutdown();
    assert(std::filesystem::exists(dir));
    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "Check directory creation"));
    std::filesystem::remove(log_file);
    std::filesystem::remove_all("testlog");
    std::cout << "test_log_file_directory_created passed" << std::endl;
}

void test_set_level_runtime() {
    std::string log_file = "testlog/test_set_level.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug should not appear");
    LogUtils::info("Info should not appear");
    LogUtils::warn("Warn should appear");

    LogUtils::set_level(LogUtils::Level::Debug);
    LogUtils::debug("Debug should appear now");
    LogUtils::info("Info should appear now");

    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(!log_file_contains(log_file, "Debug should not appear"));
    assert(!log_file_contains(log_file, "Info should not appear"));
    assert(log_file_contains(log_file, "Warn should appear"));
    assert(log_file_contains(log_file, "Debug should appear now"));
    assert(log_file_contains(log_file, "Info should appear now"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_set_level_runtime passed" << std::endl;
}


void test_fmt_info_log() {
    std::string log_file = "testlog/test_fmt_info.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::info("Hello {} Log {}", "Fmt", 123);
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "Hello Fmt Log 123"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_fmt_info_log passed" << std::endl;
}

void test_fmt_debug_level_no_output() {
    std::string log_file = "testlog/test_fmt_debug.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);
    LogUtils::init(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug {} should not appear {}", "fmt", 456);
    LogUtils::info("Info {} should appear {}", "fmt", 789);
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(!log_file_contains(log_file, "Debug fmt should not appear 456"));
    assert(log_file_contains(log_file, "Info fmt should appear 789"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_fmt_debug_level_no_output passed" << std::endl;
}

void test_fmt_warn_error_fatal_log() {
    std::string log_file = "testlog/test_fmt_warn_error_fatal.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);
    LogUtils::init(LogUtils::Level::Debug, log_file, 1024 * 1024, 1);
    LogUtils::warn("Warn {} log", "fmt");
    LogUtils::error("Error {} log {}", "fmt", 1);
    LogUtils::fatal("Fatal {} log {}", "fmt", 2);
    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "Warn fmt log"));
    assert(log_file_contains(log_file, "Error fmt log 1"));
    assert(log_file_contains(log_file, "Fatal fmt log 2"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_fmt_warn_error_fatal_log passed" << std::endl;
}

void test_fmt_set_level_runtime() {
    std::string log_file = "testlog/test_fmt_set_level.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    LogUtils::init(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
    LogUtils::debug("Debug {} should not appear", "fmt");
    LogUtils::info("Info {} should not appear", "fmt");
    LogUtils::warn("Warn {} should appear", "fmt");

    LogUtils::set_level(LogUtils::Level::Debug);
    LogUtils::debug("Debug {} should appear now", "fmt");
    LogUtils::info("Info {} should appear now", "fmt");

    LogUtils::shutdown();
    assert(std::filesystem::exists(log_file));
    assert(!log_file_contains(log_file, "Debug fmt should not appear"));
    assert(!log_file_contains(log_file, "Info fmt should not appear"));
    assert(log_file_contains(log_file, "Warn fmt should appear"));
    assert(log_file_contains(log_file, "Debug fmt should appear now"));
    assert(log_file_contains(log_file, "Info fmt should appear now"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_fmt_set_level_runtime passed" << std::endl;
}

void test_logger_guard_basic() {
    std::string log_file = "testlog/test_logger_guard.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    {
        LogUtils::LoggerGuard guard(LogUtils::Level::Info, log_file, 1024 * 1024, 1);
        LogUtils::info("LoggerGuard info message");
        LogUtils::warn("LoggerGuard warn message");
    }

    assert(std::filesystem::exists(log_file));
    assert(log_file_contains(log_file, "LoggerGuard info message"));
    assert(log_file_contains(log_file, "LoggerGuard warn message"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_logger_guard_basic passed" << std::endl;
}

void test_logger_guard_set_level() {
    std::string log_file = "testlog/test_logger_guard_level.log";
    if (std::filesystem::exists(log_file)) std::filesystem::remove(log_file);

    {
        LogUtils::LoggerGuard guard(LogUtils::Level::Warn, log_file, 1024 * 1024, 1);
        LogUtils::info("Should not appear");
        LogUtils::warn("Should appear");
        guard.set_level(LogUtils::Level::Info);
        LogUtils::info("Should appear now");
    }

    assert(std::filesystem::exists(log_file));
    assert(!log_file_contains(log_file, "Should not appear"));
    assert(log_file_contains(log_file, "Should appear"));
    assert(log_file_contains(log_file, "Should appear now"));
    std::filesystem::remove(log_file);
    std::filesystem::remove("testlog");
    std::cout << "test_logger_guard_set_level passed" << std::endl;
}

int main() {
    test_init_and_info_log();
    test_debug_level_no_output();
    test_warn_error_fatal_log();
    test_log_file_directory_created();
    test_set_level_runtime();
    test_fmt_info_log();
    test_fmt_debug_level_no_output();
    test_fmt_warn_error_fatal_log();
    test_fmt_set_level_runtime();
    test_logger_guard_basic();
    test_logger_guard_set_level();

    std::cout << "All LogUtils tests passed!" << std::endl;
    return 0;
}