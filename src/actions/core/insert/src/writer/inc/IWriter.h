#pragma once
#include <variant>
#include <chrono>
#include "FormatResult.h"
#include "InsertDataConfig.h"

class IWriter {
public:
    virtual ~IWriter() = default;
    
    // 连接/打开资源
    virtual bool connect() = 0;
    
    // Prepare for write operation
    virtual bool prepare(const std::string& sql) = 0;

    // 执行写入操作
    virtual void write(const BaseInsertData& data) = 0;
    
    // 关闭/释放资源
    virtual void close() noexcept = 0;
    
    // 获取时间戳精度
    virtual std::string get_timestamp_precision() const = 0;
};