#pragma once

#include "ColumnsCSVReader.hpp"
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>


class CSVFileCache {
public:
    using TableMap = std::unordered_map<std::string, TableData>;

    const TableMap& get_data(const ColumnsCSV& config, const std::optional<ColumnConfigInstanceVector>& instances) {
        std::call_once(once_flag_, [this, &config, &instances]() {
            try {
                auto columns_csv = std::make_unique<ColumnsCSVReader>(config, instances);
                data_ = columns_csv->generate();
            } catch (const std::exception& e) {
                error_ = e.what();
            }
        });

        if (error_) {
            throw std::runtime_error("Failed to load CSV file '" + config.file_path + "': " + *error_);
        }
        return data_;
    }

private:
    std::once_flag once_flag_;
    TableMap data_;
    std::optional<std::string> error_;
};


class CSVDataManager {
public:
    static CSVDataManager& instance() {
        static CSVDataManager manager;
        return manager;
    }

    std::shared_ptr<CSVFileCache> get_cache_for_file(const std::string& file_path) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = file_caches_.find(file_path);
        if (it == file_caches_.end()) {
            it = file_caches_.emplace(file_path, std::make_shared<CSVFileCache>()).first;
        }
        return it->second;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        file_caches_.clear();
    }

private:
    CSVDataManager() = default;
    ~CSVDataManager() = default;
    CSVDataManager(const CSVDataManager&) = delete;
    CSVDataManager& operator=(const CSVDataManager&) = delete;

    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<CSVFileCache>> file_caches_;
};