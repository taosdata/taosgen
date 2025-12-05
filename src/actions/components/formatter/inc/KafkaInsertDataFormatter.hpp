#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "KeyGenerator.hpp"
#include "RowSerializer.hpp"
#include "taos.h"
#include <nlohmann/json.hpp>

class KafkaInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit KafkaInsertDataFormatter(const DataFormat& format) : format_(format) {}

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances) override;

    FormatResult format(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        MemoryPool::MemoryBlock* batch, bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;

    KafkaInsertData format_json(const ColumnConfigInstanceVector& col_instances, MemoryPool::MemoryBlock* batch) const;

    KafkaInsertData format_influx(const ColumnConfigInstanceVector& col_instances, MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "kafka",
            [](const DataFormat& format) {
                return std::make_unique<KafkaInsertDataFormatter>(format);
            });
        return true;
    }();
};