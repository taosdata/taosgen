#pragma once
#include "BaseWriter.hpp"
#include "MqttClient.hpp"

class MqttWriter : public BaseWriter {
public:
    explicit MqttWriter(const InsertDataConfig& config, const ColumnConfigInstanceVector& col_instances);
    ~MqttWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    void write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_client(std::unique_ptr<MqttClient> client);
    MqttClient* get_client();

private:
    template<typename T>
    bool handle_insert(const T& data);

    std::unique_ptr<MqttClient> client_;
};