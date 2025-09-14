#include "CreateDatabaseAction.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include "CheckpointAction.hpp"
#include <iostream>

void CreateDatabaseAction::prepare_connector() {
    connector_ = ConnectorFactory::create(
        config_.tdengine
    );
}

void CreateDatabaseAction::execute() {
    std::cout << "Creating database: " << config_.tdengine.database << std::endl;

    try {
        prepare_connector();

        auto formatter = FormatterFactory::instance().create_formatter<CreateDatabaseConfig>(DataFormat());

        if (CheckpointAction::is_recover(global_, config_.checkpoint_info)) {
            config_.tdengine.drop_if_exists = false;
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
