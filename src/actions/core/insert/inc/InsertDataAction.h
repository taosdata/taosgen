#pragma once

#include <iostream>
#include "Barrier.h"
#include "ActionBase.h"
#include "ActionFactory.h"
#include "InsertDataConfig.h"
#include "DatabaseConnector.h"
#include "ColumnConfigInstance.h"
#include "TableDataManager.h"
#include "DataPipeline.h"
#include "FormatResult.h"
#include "IWriter.h"
#include "GarbageCollector.h"


class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const GlobalConfig& global, const InsertDataConfig& config) : global_(global), config_(config) {}

    void execute() override;


private:
    const GlobalConfig& global_;
    InsertDataConfig config_;

    void set_realtime_priority();
    void set_thread_affinity(size_t thread_id, bool reverse = false, const std::string& purpose = "");

    ColumnConfigInstanceVector create_column_instances(const InsertDataConfig& config) const;
    void print_writer_times(const std::vector<std::unique_ptr<IWriter>>& writers);

    void producer_thread_function(
        size_t producer_id,
        const std::vector<std::string>& assigned_tables,
        const ColumnConfigInstanceVector& col_instances,
        DataPipeline<FormatResult>& pipeline,
        std::shared_ptr<TableDataManager> data_manager);

    void consumer_thread_function(
        size_t consumer_id,
        DataPipeline<FormatResult>& pipeline,
        std::atomic<bool>& running,
        IWriter* writer,
        GarbageCollector<FormatResult>& gc,
        Barrier& sync_barrier);

    // Register InsertDataAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "actions/insert-data",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
            });
        return true;
    }();
};