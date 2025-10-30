#pragma once
#include <string>

namespace LogUtils {

enum class Level {
    Debug,
    Info,
    Warn,
    Error,
    Fatal
};

// Initialize the log system
void init(Level level = Level::Info,
          const std::string& log_file = "log/taosgen.log",
          size_t max_file_size = 1024 * 1024 * 5,
          size_t max_files = 3);

void shutdown();
void set_level(Level level);

// Log interface
void debug(const std::string& msg);
void info(const std::string& msg);
void warn(const std::string& msg);
void error(const std::string& msg);
void fatal(const std::string& msg);

}