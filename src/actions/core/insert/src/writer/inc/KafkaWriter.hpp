#pragma once
#include "BaseWriter.hpp"
#include "KafkaClient.hpp"

class KafkaWriter : public BaseWriter {
public:
    explicit KafkaWriter(const InsertDataConfig& config,
                         const ColumnConfigInstanceVector& col_instances,
                         const ColumnConfigInstanceVector& tag_instances,
                         size_t no = 0);
    ~KafkaWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool select_db(const std::string& db_name) override;
    bool prepare(const std::string& sql) override;
    bool write(const BaseInsertData& data) override;
    void close() noexcept override;

    void set_client(std::unique_ptr<KafkaClient> client);
    KafkaClient* get_client();

private:
    template<typename T>
    bool handle_insert(const T& data);

    std::unique_ptr<KafkaClient> client_;
};