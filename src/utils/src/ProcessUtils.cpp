#include "ProcessUtils.h"
#include <fstream>
#include <string>
#include <sstream>
#include <atomic>
#include <mutex>
#include <vector>
#include <thread>
#include <unistd.h>

namespace ProcessUtils {

double get_memory_usage_mb() {
    std::ifstream status("/proc/self/status");
    if (!status) return -1.0;
    
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            long value = 0;
            iss >> key >> value;
            return value / 1024.0;
        }
    }
    return -1.0;
}

int get_thread_count() {
    std::ifstream status("/proc/self/status");
    if (!status) return -1;
    
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            int value = 0;
            iss >> key >> value;
            return value;
        }
    }
    return -1;
}

double get_cpu_usage_percent() {
    static std::mutex mtx;
    static long prev_proc = 0;
    static std::chrono::steady_clock::time_point prev_time = std::chrono::steady_clock::now();
    
    // Get current process CPU time
    long proc = 0;
    std::ifstream self_stat("/proc/self/stat");
    if (!self_stat) return -1.0;
    
    std::string self_line;
    std::getline(self_stat, self_line);
    std::istringstream self_iss(self_line);
    std::vector<std::string> fields;
    std::string field;
    
    while (self_iss >> field) {
        fields.push_back(field);
    }
    
    if (fields.size() > 21) {
        try {
            long utime = std::stol(fields[13]);  // utime at position 14
            long stime = std::stol(fields[14]);  // stime at position 15
            proc = utime + stime;
        } catch (...) {
            return -1.0;
        }
    } else {
        return -1.0;
    }
    
    // Get current time
    auto now = std::chrono::steady_clock::now();
    
    // Calculate CPU usage
    std::lock_guard<std::mutex> lock(mtx);
    double percent = 0.0;
    
    if (prev_proc != 0) {
        // Calculate time interval (seconds)
        double interval = std::chrono::duration<double>(now - prev_time).count();
        
        if (interval > 0) {
            // Get system clock tick frequency (HZ)
            static long hz = sysconf(_SC_CLK_TCK);
            if (hz <= 0) hz = 100;  // Default value
            
            // Calculate CPU time delta (seconds)
            double proc_delta = (proc - prev_proc) / static_cast<double>(hz);
            
            // Calculate CPU usage (to match top command)
            percent = (proc_delta / interval) * 100.0;
        }
    }
    
    // Update state
    prev_proc = proc;
    prev_time = now;
    
    return percent;
}

} // namespace ProcessUtils