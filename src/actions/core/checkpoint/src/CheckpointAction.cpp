#include "CheckpointAction.hpp"
#include <chrono>
#include <fstream>
#include <cstdio> // For std::remove
#include <nlohmann/json.hpp>

std::atomic<bool> CheckpointAction::global_stop_flag_{false};
std::atomic<bool> CheckpointAction::global_interrupt_flag_{false};
std::mutex CheckpointAction::global_mutex_;
std::condition_variable CheckpointAction::global_cv_;
std::atomic<int> CheckpointAction::active_threads_count_{0};

CheckpointAction::~CheckpointAction() {
    if (timer_thread_.joinable()) {
        timer_thread_.join();   // Wait for the thread to finish
    }
}

void CheckpointAction::execute() {
    if (config_.enabled) {
        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            active_threads_count_++;
        }
        // Start the timer in a separate thread
        timer_thread_ = std::thread([this]() {
            std::cout << "[Checkpoint] Timer thread started" << std::endl;
            this->run_timer();
        });

        std::cout << "Starting checkpoint timer with an interval of "
            << config_.interval_sec << " seconds." << std::endl;
    }
}

void CheckpointAction::run_timer() {
    const int interval_sec = config_.interval_sec;
    // Loop until stop_flag_ is set to true
    while (!global_stop_flag_.load()) {
        // Wait for the specified interval
        std::this_thread::sleep_for(std::chrono::seconds(interval_sec));
        try {
            save_checkpoint();
        } catch (const std::exception& e) {
            std::cerr << "[Checkpoint] Error saving checkpoint: " << e.what() << std::endl;
        }
    }

    if (!global_interrupt_flag_.load()) {
        try {
            delete_checkpoint();
        } catch (const std::exception& e) {
            std::cerr << "[Checkpoint] Error deleting checkpoint: " << e.what() << std::endl;
        }
    }

    {
        std::lock_guard<std::mutex> lock(global_mutex_);
        active_threads_count_--; // Decrement thread count
    }

    global_cv_.notify_all();
    std::cout << "Checkpoint timer stopped." << std::endl;
}


void CheckpointAction::wait_for_all_to_stop() {
    std::unique_lock<std::mutex> lock(global_mutex_);
    // Wait until active_threads_count_ becomes 0
    global_cv_.wait(lock, [] { return active_threads_count_ == 0; });
}

void CheckpointAction::stop_all(bool is_interrupt) {
    if (is_interrupt) {
        global_interrupt_flag_.store(true);
        std::cout << "CheckpointAction received interrupt signal, will not delete checkpoints." << std::endl;
    }
    global_stop_flag_.store(true);
    global_cv_.notify_all();
    CheckpointAction::wait_for_all_to_stop();
}

void CheckpointAction::save_checkpoint() {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (checkpoint_map_.empty() || static_cast<size_t>(config_.tableCount) > checkpoint_map_.size()) {
        std::cout << "[Checkpoint] No progress data to save." << checkpoint_map_.size() << std::endl;
        return;
    }
    // Find the entry with the smallest last_checkpoint_time
    auto min_it = std::min_element(
        checkpoint_map_.begin(),
        checkpoint_map_.end(),
        [](const auto& a, const auto& b) {
            return a.second.last_checkpoint_time < b.second.last_checkpoint_time;
        }
    );
    std::string min_table_name = min_it->first;
    // Save checkpoint_data to a file or database
    std::string file_path = global_.yaml_cfg_dir + "_"+ global_.database_info.name+ "_"+ global_.super_table_info.name + "_checkpoints.json"; // Example file path
    nlohmann::json json_data;
    json_data["table_name"] = min_table_name;
    json_data["last_checkpoint_time"] = min_it->second.last_checkpoint_time;
    std::ofstream ofs(file_path);
    if (ofs) {
        ofs << json_data.dump(4) << std::endl; // Pretty print with 4 spaces indentation
        ofs.close();
        std::cout << "[Checkpoint] Saved progress for table '" << min_table_name
                    << "' at timestamp " << min_it->second.last_checkpoint_time
                    << " with write count " << min_it->second.writeCount
                    << " starting from " << config_.start_timestamp
                    << " write count " << (min_it->second.last_checkpoint_time - config_.start_timestamp) / config_.timestamp_step + 1
                    << " to " << file_path << std::endl;
    } else {
        std::cerr << "[Checkpoint] Failed to open file for writing: " << file_path << std::endl;
    }
}

void CheckpointAction::notify(const std::any& payload) {
    if (payload.type() == typeid(std::reference_wrapper<const std::vector<CheckpointData>>)) {
        try {
            const auto& wrapper = std::any_cast<std::reference_wrapper<const std::vector<CheckpointData>>>(payload);
            update_checkpoint(wrapper.get());
        } catch (const std::bad_any_cast& e) {

            std::cerr << "[Checkpoint] update_checkpoint call failed: " << e.what() << std::endl;
        }
    }
}


void CheckpointAction::update_checkpoint(const std::vector<CheckpointData>& data_list) {
    if (data_list.empty()) {
       std::cout << "[Checkpoint] No data provided for update." << std::endl;
       return;
    }

    std::lock_guard<std::mutex> lock(map_mutex_);
    for (const auto& data : data_list) {
        auto it = checkpoint_map_.find(data.table_name);
        if (it != checkpoint_map_.end()) {
            // Key exists, update it.
            it->second.last_checkpoint_time = data.last_checkpoint_time;
        } else {
            // Key doesn't exist, insert new entry.
            checkpoint_map_[data.table_name] = data;
        }
    }
    // std::cout << "[Checkpoint] Updated checkpoint data for " << data_list.size() << " entries. Current map size: " << checkpoint_map_.size() << std::endl;
}

bool CheckpointAction::is_recover(const GlobalConfig& global, const CheckpointInfo& config) {
    if (!config.enabled) {
        return false;
    }

    std::string file_path = global.yaml_cfg_dir + "_" + global.database_info.name + "_" + global.super_table_info.name + "_checkpoints.json"; // Example file path

    std::ifstream ifs(file_path);
    if (!ifs) {
        return false; // Return if file doesn't exist
    }
    ifs.close();
    return true;
}

void CheckpointAction::checkpoint_recover(const GlobalConfig& global, InsertDataConfig& config) {
    if (config.target_type != "tdengine" || !config.checkpoint_info.enabled) {
        return;
    }

    std::string file_path = global.yaml_cfg_dir + "_" + global.database_info.name + "_" + global.super_table_info.name + "_checkpoints.json"; // Example file path

    std::ifstream ifs(file_path);
    if (!ifs) {
        return; // Return if file doesn't exist
    }

    nlohmann::json json_data;
    ifs >> json_data;
    ifs.close();

    CheckpointData checkpoint_data;
    int64_t start_timestamp = TimestampUtils::parse_timestamp(
        config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp,
        config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_precision
    );

    int timestamp_step = std::get<Timestamp>(config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.timestamp_step);

    checkpoint_data.table_name = json_data["table_name"].get<std::string>();
    checkpoint_data.last_checkpoint_time = json_data["last_checkpoint_time"].get<std::int64_t>();
    checkpoint_data.writeCount = (checkpoint_data.last_checkpoint_time - start_timestamp) / timestamp_step;
    if (config.schema.generation.per_table_rows <= checkpoint_data.writeCount) {
        return;
    }
    config.schema.columns_cfg.generator.timestamp_strategy.timestamp_config.start_timestamp = (Timestamp)checkpoint_data.last_checkpoint_time;
    config.schema.generation.per_table_rows -= checkpoint_data.writeCount;
}

void CheckpointAction::delete_checkpoint() {
    std::lock_guard<std::mutex> lock(map_mutex_);
    std::string file_path = global_.yaml_cfg_dir + "_" + global_.database_info.name + "_" + global_.super_table_info.name + "_checkpoints.json";
    std::remove(file_path.c_str());
    checkpoint_map_.clear();
    std::cout << "[Checkpoint] delete checkpoint file and cleared all in-memory checkpoint data." << std::endl;
}
