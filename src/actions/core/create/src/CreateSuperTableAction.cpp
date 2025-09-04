#include "CreateSuperTableAction.hpp"
#include "FormatterRegistrar.hpp"
#include "ConnectorFactory.hpp"
#include <iostream>

void CreateSuperTableAction::prepare_connector() {
    connector_ = ConnectorFactory::create(
        config_.tdengine
    );
}

void CreateSuperTableAction::execute() {
    std::cout << "Creating super table: " << config_.tdengine.database << "." << config_.schema.name << std::endl;

    try {
        prepare_connector();

        auto formatter = FormatterFactory::instance().create_formatter<CreateSuperTableConfig>(DataFormat());
        FormatResult formatted_result = formatter->format(config_);
        connector_->execute(std::get<std::string>(formatted_result));

    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        throw;
    }

    connector_->close();
}
