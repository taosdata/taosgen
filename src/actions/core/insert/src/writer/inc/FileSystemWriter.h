#pragma once
#include "IWriter.h"
#include "TimeIntervalStrategy.h"
#include <fstream>
#include <filesystem>

class FileSystemWriter : public IWriter {
public:
    FileSystemWriter(const InsertDataConfig::Target& target);
    ~FileSystemWriter();
    
    bool connect() override;
    void write(const BaseInsertData& data) override;
    void close() noexcept override;
    
    void set_time_interval_config(
        const InsertDataConfig::Control::TimeInterval& config) override;
    
    std::string get_timestamp_precision() const override;

private:
    // 获取当前文件路径
    std::filesystem::path get_current_file_path(int64_t timestamp);
    
    // 写入CSV内容
    void write_csv(const std::string& data, int64_t timestamp);
    
    // 时间间隔策略执行器
    TimeIntervalStrategy time_strategy_;
    
    // 文件输出流
    std::ofstream output_file_;
    
    // 配置信息
    InsertDataConfig::Target target_config_;
    
    // 当前文件信息
    std::string current_file_path_;
    int64_t current_file_end_time_ = 0;
};