#include "CreateDatabaseAction.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
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

        FormatResult result = formatter->format(config_);
        const auto& stmts = std::get<std::vector<std::string>>(result);

        for (const auto& stmt : stmts) {
            connector_->execute(stmt);
        }
    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        throw;
    }

    connector_->close();
}
