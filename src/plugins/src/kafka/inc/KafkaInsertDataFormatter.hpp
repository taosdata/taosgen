#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "KafkaInsertData.hpp"
#include "KafkaFormatOptions.hpp"
#include "KeyGenerator.hpp"
#include "RowSerializer.hpp"
#include <nlohmann/json.hpp>

class KafkaInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit KafkaInsertDataFormatter(const DataFormat& format);

    std::string prepare(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances) override;

    FormatResult format(const InsertDataConfig& config,
                        const ColumnConfigInstanceVector& col_instances,
                        const ColumnConfigInstanceVector& tag_instances,
                        MemoryPool::MemoryBlock* batch,
                        bool is_checkpoint_recover = false) const override;

private:
    const DataFormat& format_;
    const KafkaFormatOptions* format_options_;

    FormatResult format_json(const ColumnConfigInstanceVector& col_instances,
                             const ColumnConfigInstanceVector& tag_instances,
                             MemoryPool::MemoryBlock* batch) const;

    FormatResult format_influx(const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances,
                               MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::register_formatter<InsertDataConfig>(
            "kafka",
            [](const DataFormat& format) {
                return std::make_unique<KafkaInsertDataFormatter>(format);
            });
        return true;
    }();
};