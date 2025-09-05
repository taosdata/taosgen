#include "CreateDatabaseAction.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include "CheckpointAction.hpp"
#include <iostream>

void CreateDatabaseAction::prepare_connector() {
    connector_ = ConnectorFactory::create(
        config_.data_channel,
        config_.connection_info
    );
}

void CreateDatabaseAction::execute() {
    std::cout << "Creating database: " << config_.database_info.name << std::endl;

    try {
        prepare_connector();

        auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(config_.data_format);
        
        if (CheckpointAction::is_recover(global_, config_.checkpoint_info)) {
            config_.database_info.drop_if_exists = false;
            std::cout << "[Info] Checkpoint file exists. Skipping database drop." << std::endl;
        }
        
        FormatResult result = formatter->format(config_);
        const auto& stmts = std::get<std::vector<std::string>>(result);

        for (const auto& stmt : stmts) {
            if (!connector_->execute(stmt)) {
                throw std::runtime_error("Failed to execute SQL: " + stmt);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        throw;
    }

    connector_->close();
}
