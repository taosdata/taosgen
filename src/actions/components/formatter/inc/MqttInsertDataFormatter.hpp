#pragma once

#include "IFormatter.hpp"
#include "FormatterFactory.hpp"
#include "TopicGenerator.hpp"
#include "Compressor.hpp"
#include "EncodingConverter.hpp"
#include "RowSerializer.hpp"
#include "taos.h"
#include <nlohmann/json.hpp>

class MqttInsertDataFormatter final : public IInsertDataFormatter {
public:
    explicit MqttInsertDataFormatter(const DataFormat& format) : format_(format) {}

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

    MqttInsertData format_json(const ColumnConfigInstanceVector& col_instances,
                               const ColumnConfigInstanceVector& tag_instances,
                               MemoryPool::MemoryBlock* batch) const;

    inline static bool registered_ = []() {
        FormatterFactory::instance().register_formatter<InsertDataConfig>(
            "mqtt",
            [](const DataFormat& format) {
                return std::make_unique<MqttInsertDataFormatter>(format);
            });
        return true;
    }();
};