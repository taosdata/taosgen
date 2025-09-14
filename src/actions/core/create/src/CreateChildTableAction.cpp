#include "CreateChildTableAction.hpp"
#include <iostream>
#include <thread>
#include "FormatterRegistrar.hpp"
#include "TableNameGenerator.hpp"
#include "TableNameCSVReader.hpp"
#include "RowGenerator.hpp"
#include "TagsCSVReader.hpp"
#include "ConnectorSource.hpp"


void CreateChildTableAction::execute() {
    std::cout << "Creating child table: " << config_.tdengine.database << "." << config_.schema.name << std::endl;

    try {
        // Generate table names
        std::vector<std::string> table_names;
        if (config_.schema.tbname.source_type == "generator") {
            TableNameGenerator generator(config_.schema.tbname.generator);
            table_names = generator.generate();
            // for (const auto& name : table_names) {
            //     std::cout << "Generated table name: " << name << std::endl;
            // }
        } else if (config_.schema.tbname.source_type == "csv") {
            TableNameCSVReader csv_reader(config_.schema.tbname.csv);
            table_names = csv_reader.generate();
            for (const auto& name : table_names) {
                std::cout << "Read table name from CSV: " << name << std::endl;
            }
        } else {
            throw std::runtime_error("Unsupported table name source type: " + config_.schema.tbname.source_type);
        }

        std::cout << "Total table names generated: " << table_names.size() << std::endl;


        // Generate tags
        std::vector<RowType> tags;
        if (config_.schema.tags_cfg.source_type == "generator") {
            auto instances = ColumnConfigInstanceFactory::create(config_.schema.tags_cfg.get_schema());
            RowGenerator row_generator(instances);
            tags = row_generator.generate(table_names.size());
            // for (const auto& tag : tags) {
            //     std::cout << "Generated tag: " << tag << std::endl;
            // }
        } else if (config_.schema.tags_cfg.source_type == "csv") {
            auto instances = ColumnConfigInstanceFactory::create(config_.schema.tags_cfg.get_schema());
            TagsCSVReader tags_csv(config_.schema.tags_cfg.csv, instances);
            tags = tags_csv.generate();
            for (const auto& tag : tags) {
                std::cout << "Read tag from CSV: " << tag << std::endl;
            }
            if (tags.size() != table_names.size()) {
                throw std::runtime_error("Number of tags does not match number of table names");
            }
        } else {
            throw std::runtime_error("Unsupported tags source type: " + config_.schema.tags_cfg.source_type);
        }

        std::cout << "Total tags generated: " << tags.size() << std::endl;


        // Split data into groups based on concurrency
        int concurrency = config_.batch.concurrency;
        int total_tables = table_names.size();
        int tables_per_group = (total_tables + concurrency - 1) / concurrency;

        ConnectorSource conn_source(config_.tdengine);
        std::vector<std::thread> threads;
        for (int group_idx = 0; group_idx < concurrency; ++group_idx) {
            int start_idx  = group_idx * tables_per_group;
            int end_idx    = std::min(start_idx + tables_per_group, total_tables);

            if (start_idx >= total_tables) break;

            // Extract the subset of table names and tags for this group
            std::vector<std::string> group_table_names(table_names.begin() + start_idx, table_names.begin() + end_idx);
            std::vector<RowType> group_tags(tags.begin() + start_idx, tags.begin() + end_idx);

            // Create threads for each group
            threads.emplace_back([this, group_idx, group_table_names, group_tags, &conn_source]() {
                try {
                    // Create a local connector
                    auto local_connector = conn_source.get_connector();

                    // Split into batches based on batch size
                    int batch_size = config_.batch.size;
                    int total_batches = (group_table_names.size() + batch_size - 1) / batch_size;

                    auto formatter = FormatterFactory::instance().create_formatter<CreateChildTableConfig>(DataFormat());

                    for (int batch_idx = 0; batch_idx < total_batches; ++batch_idx) {
                        int batch_start = batch_idx * batch_size;
                        int batch_end = std::min(batch_start + batch_size, static_cast<int>(group_table_names.size()));

                        std::vector<std::string> batch_table_names(group_table_names.begin() + batch_start, group_table_names.begin() + batch_end);
                        std::vector<RowType> batch_tags(group_tags.begin() + batch_start, group_tags.begin() + batch_end);

                        // Format the batch data
                        FormatResult formatted_result = formatter->format(config_, batch_table_names, batch_tags);

                        // std::cout << "Formatted result for batch " << group_idx << '#' << batch_idx << ": "
                        //           << std::get<std::string>(formatted_result) << std::endl;

                        // Execute the formatted result
                        local_connector->execute(std::get<std::string>(formatted_result));

                        bool result = local_connector->execute(std::get<std::string>(formatted_result));
                        if (!result) {
                            throw std::runtime_error("Failed to execute SQL for batch " + std::to_string(group_idx) + "#" + std::to_string(batch_idx));
                        }
                    }

                    local_connector->close();
                } catch (const std::exception& e) {
                    // std::cerr << "Error in thread: " << e.what() << std::endl;
                    throw;
                }
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }
    } catch (const std::exception& e) {
        std::cerr << "An error occurred: " << e.what() << std::endl;
        throw;
    }
}
