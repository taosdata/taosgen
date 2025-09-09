#pragma once

#include "Barrier.hpp"
#include "ActionBase.hpp"
#include "ActionFactory.hpp"
#include "InsertDataConfig.hpp"
#include "DatabaseConnector.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableDataManager.hpp"
#include "DataPipeline.hpp"
#include "FormatResult.hpp"
#include "IWriter.hpp"
#include "GarbageCollector.hpp"
#include <iostream>

class InsertDataAction : public ActionBase {
public:
    explicit InsertDataAction(const GlobalConfig& global, const InsertDataConfig& config)
            : global_(global), config_(config) {
        col_instances_ = create_column_instances();
    }

    void execute() override;


private:
    const GlobalConfig& global_;
    InsertDataConfig config_;
    ColumnConfigInstanceVector col_instances_;

    void set_realtime_priority();
    void set_thread_affinity(size_t thread_id, bool reverse = false, const std::string& purpose = "");

    ColumnConfigInstanceVector create_column_instances() const;
    void print_writer_times(const std::vector<std::unique_ptr<IWriter>>& writers);

    void producer_thread_function(
        size_t producer_id,
        const std::vector<std::string>& assigned_tables,
        DataPipeline<FormatResult>& pipeline,
        std::shared_ptr<TableDataManager> data_manager);

    void consumer_thread_function(
        size_t consumer_id,
        DataPipeline<FormatResult>& pipeline,
        std::atomic<bool>& running,
        IWriter* writer,
        std::optional<ConnectorSource>& conn_source,
        GarbageCollector<FormatResult>& gc,
        Barrier& sync_barrier);

    // Register InsertDataAction to ActionFactory
    inline static bool registered_ = []() {
        ActionFactory::instance().register_action(
            "tdengine/insert-data",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
            });

        ActionFactory::instance().register_action(
            "mqtt/publish-data",
            [](const GlobalConfig& global, const ActionConfigVariant& config) {
                return std::make_unique<InsertDataAction>(global, std::get<InsertDataConfig>(config));
            });
        return true;
    }();
};