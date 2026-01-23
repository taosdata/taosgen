#pragma once
#include "BaseWriter.hpp"
#include "WriterFactory.hpp"
#include "KafkaClient.hpp"

class KafkaWriter : public BaseWriter {
public:
    explicit KafkaWriter(const InsertDataConfig& config, size_t no = 0);
    ~KafkaWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_client(std::unique_ptr<KafkaClient> client);
    KafkaClient* get_client();

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);

    std::unique_ptr<KafkaClient> client_;

    inline static bool registered_ = []() {
        WriterFactory::register_writer(
            "kafka",
            [](const InsertDataConfig& config, size_t no, std::shared_ptr<ActionRegisterInfo>) {
                return std::make_unique<KafkaWriter>(config, no);
            });
        return true;
    }();
};