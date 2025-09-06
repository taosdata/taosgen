#pragma once
#include "BaseWriter.hpp"
#include "DatabaseConnector.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"

class TDengineWriter : public BaseWriter {
public:
    explicit TDengineWriter(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances, size_t no = 0, std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    ~TDengineWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool write(const BaseInsertData& data) override;
    void close() noexcept override;

private:
    template<typename T>
    bool handle_insert(const T& data);
    size_t no_;
    std::unique_ptr<DatabaseConnector> connector_;
    
};