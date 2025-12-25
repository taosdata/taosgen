#pragma once
#include "BaseWriter.hpp"
#include "WriterFactory.hpp"
#include "MqttClient.hpp"

class MqttWriter : public BaseWriter {
public:
    explicit MqttWriter(const InsertDataConfig& config, size_t no = 0);
    ~MqttWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool prepare(const std::string& context) override;
    bool write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_client(std::unique_ptr<MqttClient> client);
    MqttClient* get_client();

private:
    template<typename T>
    bool handle_insert(const T& data);

    std::unique_ptr<MqttClient> client_;

    inline static bool registered_ = []() {
        WriterFactory::register_writer(
            "mqtt",
            [](const InsertDataConfig& config, size_t no, std::shared_ptr<ActionRegisterInfo>) {
                return std::make_unique<MqttWriter>(config, no);
            });
        return true;
    }();
};