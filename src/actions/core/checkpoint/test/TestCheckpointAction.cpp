#include <iostream>
#include <cassert>
#include <fstream>
#include "CheckpointAction.hpp"
#include "GlobalConfig.hpp"
#include "CheckpointActionConfig.hpp"
#include "InsertDataConfig.hpp"
#include "CheckpointData.hpp"

#include <nlohmann/json.hpp>

void test_checkpoint_recover() {
    std::cout << "\n--- Running test_checkpoint_recover ---" << std::endl;
    GlobalConfig global;
    global.yaml_cfg_dir = "./b";
    global.tdengine.database = "test_db";
    global.schema.name = "test_super_table_recover"; // Use a different name to avoid conflict

    // 1. Manually create a checkpoint file
    std::string checkpoint_file = global.yaml_cfg_dir + "_" + global.tdengine.database + "_" + global.schema.name + "_checkpoints.json";
    std::cout << "Creating dummy checkpoint file: " << checkpoint_file << std::endl;
    nlohmann::json json_data;
    json_data["table_name"] = "t1";
    json_data["last_checkpoint_time"] = 1620000005000;

    std::ofstream ofs(checkpoint_file);
    ofs << json_data.dump(4);
    ofs.close();

    // 2. Check if is_recover works
    CheckpointInfo chk_info;
    chk_info.enabled = true;
    assert(CheckpointAction::is_recover(global, chk_info));
    std::cout << "is_recover check passed." << std::endl;

    // 3. Create InsertDataConfig and test recover
    InsertDataConfig insert_config;
    insert_config.target_type = "tdengine";
    insert_config.checkpoint_info.enabled = true;
    insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config = TimestampGeneratorConfig {
        .start_timestamp = "2021-05-03 08:00:00", // 1620000000000
        .timestamp_precision = "ms",
        .timestamp_step = 1000
    };
    insert_config.schema.generation.rows_per_table = 100;

    CheckpointAction::checkpoint_recover(global, insert_config);
    std::cout << "checkpoint_recover executed." << insert_config.schema.generation.rows_per_table << std::endl;
    // 4. Assert config changes
    int64_t expected_start_ts = 1620000005000;
    int64_t recovered_rows = 5;
    int64_t expected_rows = 100 - recovered_rows;

    (void)expected_start_ts;
    (void)expected_rows;
    std::cout << "Recovered start_timestamp: " << std::get<int64_t>(insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp) << std::endl;
    std::cout << "expected_start_ts: " << expected_start_ts << std::endl;
    std::cout << "Adjusted rows_per_table: " << insert_config.schema.generation.rows_per_table << std::endl;
    std::cout << "expected_rows: " << expected_rows << std::endl;
    assert(std::get<int64_t>(insert_config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp) == expected_start_ts);
    assert(insert_config.schema.generation.rows_per_table == expected_rows);

    // 5. Clean up
    std::remove(checkpoint_file.c_str());
    std::cout << "Cleaned up dummy checkpoint file." << std::endl;

    std::cout << "test_checkpoint_recover passed\n";
}

void test_checkpoint_interrupt() {
    std::cout << "\n--- Running test_checkpoint_interrupt ---" << std::endl;
    GlobalConfig global;
    global.yaml_cfg_dir = "./c";
    global.tdengine.database = "test_db";
    global.schema.name = "test_super_table_interrupt"; // Use a different name

    CheckpointActionConfig config;
    config.enabled = true;
    config.interval_sec = 1;
    config.tableCount = 1;

    // Create and execute action
    auto action = std::make_unique<CheckpointAction>(global, config);
    action->execute();

    // Provide data and wait for checkpoint file
    std::vector<CheckpointData> data_list = {{"table1", 1700000000000, 1}};
    action->update_checkpoint(data_list);
    std::cout << "Waiting for checkpoint file to be created..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // Trigger an interrupt
    std::cout << "Triggering interrupt..." << std::endl;
    CheckpointAction::stop_all(true); // true for interrupt

    // Check that the file was created
    std::string checkpoint_file = global.yaml_cfg_dir + "_" + global.tdengine.database + "_" + global.schema.name + "_checkpoints.json";

    // Check that the file was NOT deleted
    std::cout << "Checking if checkpoint file still exists after interrupt..." << std::endl;
    std::ifstream ifs_after_interrupt(checkpoint_file);
    assert(ifs_after_interrupt.is_open());
    ifs_after_interrupt.close();
    std::cout << "Checkpoint file correctly preserved after interrupt." << std::endl;

    // Clean up the file
    std::remove(checkpoint_file.c_str());
    std::cout << "Cleaned up dummy checkpoint file." << std::endl;

    std::cout << "test_checkpoint_interrupt passed\n";
}

int main() {
    test_checkpoint_interrupt();
    test_checkpoint_recover();

    std::cout << "All Checkpoint tests passed!\n";
    return 0;
}
