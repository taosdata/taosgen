#include "LogUtils.hpp"
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <memory>
#include <vector>
#include <filesystem>

namespace LogUtils {

static std::shared_ptr<spdlog::logger> logger;

spdlog::level::level_enum to_spdlog_level(Level level) {
    switch (level) {
        case Level::Debug: return spdlog::level::debug;
        case Level::Info:  return spdlog::level::info;
        case Level::Warn:  return spdlog::level::warn;
        case Level::Error: return spdlog::level::err;
        case Level::Fatal: return spdlog::level::critical;
        default:           return spdlog::level::info;
    }
}

void init(Level level, const std::string& log_file, size_t max_file_size, size_t max_files) {
    std::filesystem::path log_path(log_file);
    std::filesystem::path parent_dir = log_path.parent_path();

    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
        std::filesystem::create_directories(parent_dir);
    }

    spdlog::init_thread_pool(8192, 1);

    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_file, max_file_size, max_files);

    std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};
    logger = std::make_shared<spdlog::async_logger>(
        "taosgen_logger", sinks.begin(), sinks.end(),
        spdlog::thread_pool(), spdlog::async_overflow_policy::block);

    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%l] [thread %t] %v");
    logger->set_level(to_spdlog_level(level));
    logger->flush_on(spdlog::level::info);
    spdlog::flush_every(std::chrono::seconds(1));
    spdlog::register_logger(logger);
    spdlog::set_default_logger(logger);
}

void shutdown() {
    if (logger) logger->flush();
    spdlog::shutdown();
}

void set_level(Level level) {
    if (logger) logger->set_level(to_spdlog_level(level));
}

void debug(const std::string& msg) {
    if (logger) logger->debug(msg);
}

void info(const std::string& msg) {
    if (logger) logger->info(msg);
}

void warn(const std::string& msg) {
    if (logger) logger->warn(msg);
}

void error(const std::string& msg) {
    if (logger) logger->error(msg);
}

void fatal(const std::string& msg) {
    if (logger) logger->critical(msg);
}

}