#pragma once
#include "BaseWriter.hpp"
#include "WriterFactory.hpp"
#include "DatabaseConnector.hpp"
#include "CheckpointAction.hpp"
#include "ActionRegisterInfo.hpp"

class TDengineWriter : public BaseWriter {
public:
    explicit TDengineWriter(const InsertDataConfig& config,
                            size_t no = 0,
                            std::shared_ptr<ActionRegisterInfo> action_info = nullptr);
    ~TDengineWriter() override;

    bool connect(std::optional<ConnectorSource>& conn_source) override;
    bool prepare(std::unique_ptr<const ISinkContext> context) override;
    bool write(const BaseInsertData& data) override;
    void close() noexcept override;

private:
    template<typename PayloadT>
    bool handle_insert(const BaseInsertData& data);
    // size_t no_;
    std::unique_ptr<DatabaseConnector> connector_;

    inline static bool registered_ = []() {
        WriterFactory::register_writer(
            "tdengine",
            [](const InsertDataConfig& config, size_t no, std::shared_ptr<ActionRegisterInfo> action_info) {
                return std::make_unique<TDengineWriter>(config, no, std::move(action_info));
            });
        return true;
    }();
};