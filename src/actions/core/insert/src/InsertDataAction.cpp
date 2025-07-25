#include "InsertDataAction.h"
#include <sched.h>
#include <cstring>
#include <iostream>
#include <chrono>
#include <thread>
#include <variant>
#include <type_traits>
#include <pthread.h>
#include <iomanip> 
#include "FormatterRegistrar.h"
#include "FormatterFactory.h"
#include "TableNameManager.h"
#include "TableDataManager.h"
#include "WriterFactory.h"
#include "TimeRecorder.h"
#include "ProcessUtils.h"


void set_realtime_priority() {
#if defined(__linux__) || defined(__APPLE__)
    struct sched_param param;
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) != 0) {
        std::cerr << "Warning: Failed to set real-time priority. "
                  << "Requires root privileges or CAP_SYS_NICE capability.";
    }
#endif
}

void set_thread_affinity(size_t thread_id, bool reverse = false, const std::string& purpose = "") {
#if defined(__linux__) || defined(__APPLE__)
    // Get available CPU cores
    unsigned int num_cores = std::thread::hardware_concurrency();
    if (num_cores == 0) num_cores = 1;

    // Calculate core ID, supporting forward or reverse binding
    size_t core_id = reverse
        ? (num_cores - 1 - (thread_id % num_cores))
        : (thread_id % num_cores);

    // Set affinity
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Warning: Failed to set thread affinity for thread "
                  << thread_id << " to core " << core_id
                  << " (error code: " << rc << ", "
                  << strerror(rc) << ")" << std::endl;
        return;
    }

    std::cout << (purpose.empty() ? "" : (purpose + " ")) << "Thread " << thread_id << " bound to core " << core_id
              << (reverse ? " (reverse binding)" : " (forward binding)") << std::endl;
#endif
}

void InsertDataAction::execute() {
    std::cout << "Inserting data into: " << config_.target.target_type;
    if (config_.target.target_type == "tdengine") {
        std::cout << " @ "
                  << config_.target.tdengine.connection_info.host << ":"
                  << config_.target.tdengine.connection_info.port << std::endl;
    } else if (config_.target.target_type == "file_system") {
        std::cout << " @ " << config_.target.file_system.output_dir << std::endl;
    } else {
        throw std::invalid_argument("Unsupported target type: " + config_.target.target_type);
    }

    try {
        // 1. Generate all child table names and split by producer thread count
        TableNameManager name_manager(config_);
        auto all_names = name_manager.generate_table_names();
        
        // Check generated table names
        if (all_names.empty()) {
            throw std::runtime_error("No table names were generated");
        }
        
        const auto split_names = name_manager.split_for_threads();
        
        // Check split result
        if (split_names.size() != config_.control.data_generation.generate_threads) {
            throw std::runtime_error(
                "Split names count (" + std::to_string(split_names.size()) + 
                ") does not match generate_threads (" + 
                std::to_string(config_.control.data_generation.generate_threads) + ")"
            );
        }

        // Print assignment info
        for (size_t i = 0; i < split_names.size(); i++) {
            std::cout << "Producer thread " << i << " will handle " 
                     << split_names[i].size() << " tables" << std::endl;
        }
        
        // 2. Create column config instances
        auto col_instances = create_column_instances(config_);

        // 3. Initialize data pipeline
        const size_t producer_thread_count = config_.control.data_generation.generate_threads;
        const size_t consumer_thread_count = config_.control.insert_control.insert_threads;
        const size_t queue_capacity = config_.control.data_generation.queue_capacity;
        const double queue_warmup_ratio = config_.control.data_generation.queue_warmup_ratio;
        const size_t per_request_rows = config_.control.insert_control.per_request_rows;
        const size_t interlace_rows = config_.control.data_generation.interlace_mode.rows;
        const int64_t per_table_rows = config_.control.data_generation.per_table_rows;
        
        size_t block_count = (queue_capacity + 2) * producer_thread_count;
        size_t max_tables_per_block = std::min(name_manager.chunk_size(), per_request_rows);
        size_t max_rows_per_table = per_table_rows;

        if (config_.control.data_generation.interlace_mode.enabled) {
            max_tables_per_block = (per_request_rows + interlace_rows - 1) / interlace_rows;
            max_rows_per_table = std::min(max_rows_per_table, interlace_rows);

        } else {
            max_tables_per_block = (per_request_rows + per_table_rows - 1) / per_table_rows;
            max_rows_per_table = per_table_rows;
        }

        MemoryPool pool(block_count, max_tables_per_block, max_rows_per_table, col_instances);

        // Create data pipeline
        DataPipeline<FormatResult> pipeline(producer_thread_count, consumer_thread_count, queue_capacity);
        Barrier sync_barrier(consumer_thread_count + 1);

        // 4. Start consumer threads
        std::vector<std::thread> consumer_threads;
        consumer_threads.reserve(consumer_thread_count);

        std::vector<std::unique_ptr<IWriter>> writers;
        writers.reserve(consumer_thread_count);

        const size_t group_size = 10;
        GarbageCollector<FormatResult> gc((consumer_thread_count + group_size - 1) / group_size);

        // Create all writer instances
        auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config_.control.data_format);
        auto sql = formatter->prepare(config_, col_instances, 1);
        for (size_t i = 0; i < consumer_thread_count; i++) {
            writers.push_back(WriterFactory::create(config_));
            
            // Connect to database
            if (!writers[i]->connect()) {
                throw std::runtime_error("Failed to connect writer for thread " + std::to_string(i));
            }

            if (config_.target.target_type == "tdengine") {
                if (!writers[i]->select_db(config_.target.tdengine.database_info.name)) {
                    throw std::runtime_error("Failed to select database for writer thread " + std::to_string(i) + \
                        " with database name: " + config_.target.tdengine.database_info.name);  
                }
            }

            // For stmt v2 format, need to prepare
            if (config_.control.data_format.format_type == "stmt" && 
                config_.control.data_format.stmt_config.version == "v2") {
                if (!writers[i]->prepare(sql)) {
                    throw std::runtime_error("Failed to prepare writer for thread " + std::to_string(i) + \
                        " with SQL: " + sql);
                }
            }
        }

        auto consumer_running = std::make_unique<std::atomic<bool>[]>(consumer_thread_count);
        for (size_t i = 0; i < consumer_thread_count; i++) {
            consumer_running[i].store(true);
        }

        // 5. Start producer threads
        std::vector<std::shared_ptr<TableDataManager>> data_managers;
        data_managers.reserve(producer_thread_count);
        
        std::vector<std::thread> producer_threads;
        producer_threads.reserve(producer_thread_count);

        auto producer_finished = std::make_unique<std::atomic<bool>[]>(producer_thread_count);
        for (size_t i = 0; i < producer_thread_count; i++) {
            producer_finished[i].store(false);
        }

        std::atomic<size_t> active_producers(producer_thread_count);

        for (size_t i = 0; i < producer_thread_count; i++) {
            auto data_manager = std::make_shared<TableDataManager>(pool, config_, col_instances);
            data_managers.push_back(data_manager);

            producer_threads.emplace_back([this, i, &split_names, &col_instances, &pipeline, data_manager, &active_producers, &producer_finished] {
                try {
                    set_thread_affinity(i, false, "Producer");
                    set_realtime_priority();
                    producer_thread_function(i, split_names[i], col_instances, pipeline, data_manager);
                    producer_finished[i].store(true);
                } catch (const std::exception& e) {
                    std::cerr << "Producer thread " << i << " failed: " << e.what() << std::endl;
                }
                active_producers--;
            });
        }

        (void)ProcessUtils::get_cpu_usage_percent();
        std::this_thread::sleep_for(std::chrono::seconds(2));

        while (true) {
            size_t total_queued = pipeline.total_queued();
            double queue_ratio = static_cast<double>(total_queued) / (producer_thread_count * queue_capacity);

            std::cout << "[Warmup] Queue fill ratio: " << std::fixed << std::setprecision(2)
                      << (queue_ratio * 100) << "%, target: " << (queue_warmup_ratio * 100) << "%" << std::endl;
        
            if (queue_ratio >= queue_warmup_ratio) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Start consumer threads
        for (size_t i = 0; i < consumer_thread_count; i++) {

            consumer_threads.emplace_back([this, i, &pipeline, &consumer_running, &writers, &gc, &sync_barrier] {
                set_thread_affinity(i, true, "Consumer");
                set_realtime_priority();
                consumer_thread_function(i, pipeline, consumer_running[i], writers[i].get(), gc, sync_barrier);
            });
        }

        sync_barrier.arrive_and_wait();
        const auto start_time = std::chrono::steady_clock::now();

        // 6. Monitor
        size_t last_total_rows = 0;
        auto last_time = start_time;
        
        while (active_producers > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            // Calculate total rows generated by all producers
            size_t total_rows = 0;
            for (const auto& manager : data_managers) {
                total_rows += manager->get_total_rows_generated();
            }

            // Get current time
            const auto now = std::chrono::steady_clock::now();
            
            // Calculate actual interval (seconds)
            const auto interval = std::chrono::duration<double>(now - last_time).count();
            
            // Calculate real-time rate (rows/sec)
            const double rows_per_sec = interval > 0 ? 
                static_cast<double>(total_rows - last_total_rows) / interval : 0.0;
            
            // Update last values
            last_total_rows = total_rows;
            last_time = now;
            
            // Calculate total runtime
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

            std::cout << "Runtime: " << duration.count() << "s | "
                    << "Rate: " << std::fixed << std::setprecision(1) << rows_per_sec << " rows/s | "
                    << "Total: " << total_rows << " rows | "
                    << "Queue: " << pipeline.total_queued() << " items | "
                    << "CPU Usage: " << std::fixed << std::setprecision(2) << ProcessUtils::get_cpu_usage_percent() << "% | "
                    << "Memory Usage: " << ProcessUtils::get_memory_usage_human_readable() << " | "
                    << "Thread Count: " << ProcessUtils::get_thread_count() << "\n";
        }

        std::cout << "All producer threads have finished." << std::endl;
        // Wait for all data to be sent
        size_t last_queue_size = pipeline.total_queued();
        auto last_check_time = std::chrono::steady_clock::now();
        
        while (pipeline.total_queued() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            auto current_time = std::chrono::steady_clock::now();
            auto interval = std::chrono::duration<double>(current_time - last_check_time).count();
            
            if (interval >= 1.0) {
                size_t current_queue_size = pipeline.total_queued();
                double process_rate = (last_queue_size - current_queue_size) / interval;
        
                std::cout << "Remaining queue items: " << current_queue_size 
                          << " | Processing rate: " << std::fixed << std::setprecision(1) 
                          << process_rate << " items/s | "
                          << "CPU Usage: " << std::fixed << std::setprecision(2) << ProcessUtils::get_cpu_usage_percent() << "% | "
                          << "Memory Usage: " << ProcessUtils::get_memory_usage_human_readable() << " | "
                          << "Thread Count: " << ProcessUtils::get_thread_count() << "\n";
                
                last_queue_size = current_queue_size;
                last_check_time = current_time;
            }
        }

        const auto end_time = std::chrono::steady_clock::now();
        (void)end_time;

        // 7. Wait for producers to finish
        for (auto& t : producer_threads) {
            if (t.joinable()) t.join();
        }
        
        // Terminate pipeline (notify consumers)
        pipeline.terminate();
        
        // 8. Wait for consumers to finish
        // for (size_t i = 0; i < consumer_thread_count; i++) {
        //     consumer_running[i] = false;
        // }
        
        for (auto& t : consumer_threads) {
            if (t.joinable()) t.join();
        }

        // Calculate final total rows
        size_t final_total_rows = 0;
        for (const auto& manager : data_managers) {
            final_total_rows += manager->get_total_rows_generated();
        }

        if (global_.verbose) {
            print_writer_times(writers);
        }

        // Obtain the minimum value of the start_write_time() for all writers
        auto min_start_write_time = std::chrono::steady_clock::time_point::max();
        for (const auto& writer : writers) {
            auto t = writer->start_write_time();
            if (t < min_start_write_time) {
                min_start_write_time = t;
            }
        }

        // Obtain the maximum value of the end_write_time() for all writers
        std::vector<std::chrono::steady_clock::time_point> end_write_times;
        end_write_times.reserve(writers.size());
        auto max_end_write_time = std::chrono::steady_clock::time_point::min();
        for (const auto& writer : writers) {
            auto t = writer->end_write_time();
            end_write_times.push_back(t);
            if (t > max_end_write_time) {
                max_end_write_time = t;
            }
        }

        // Calculate total wait time (sum of each thread's waiting time after its own finish)
        double total_wait_time = 0.0;
        for (const auto& t : end_write_times) {
            total_wait_time += std::chrono::duration<double>(max_end_write_time - t).count();
        }
        double avg_wait_time = total_wait_time / consumer_thread_count; // average per thread

        // Calculate total duration (seconds)
        const auto total_duration = std::chrono::duration<double>(max_end_write_time - min_start_write_time).count();

        // Calculate average insert rate
        const double avg_rows_per_sec = total_duration > 0 ? 
            static_cast<double>(final_total_rows) / total_duration : 0.0;

        // Print performance statistics
        std::cout << "\n=============================================== Insert Summary Statistics ====================================================\n"
                  << "Insert Threads: " << consumer_thread_count << "\n"
                  << "Total Rows: " << final_total_rows << "\n"
                  << "Total Duration: " << std::fixed << std::setprecision(2) 
                  << total_duration << " seconds\n"
                  << "Average Rate: " << std::fixed << std::setprecision(2) 
                  << avg_rows_per_sec << " rows/second\n"
                  << "==============================================================================================================================\n";

        // Collect performance metrics
        ActionMetrics global_play_metrics;
        ActionMetrics global_write_metrics;
        
        for (const auto& writer : writers) {
            global_play_metrics.merge_from(writer->get_play_metrics());
            global_write_metrics.merge_from(writer->get_write_metrics());
        }
        
        global_play_metrics.calculate();
        global_write_metrics.calculate();

        // Print performance statistics
        double thread_latency = global_write_metrics.get_sum() / consumer_thread_count / 1000;
        double effective_ratio = thread_latency / total_duration * 100.0;
        double framework_ratio = (1 - (thread_latency + avg_wait_time) / total_duration) * 100.0;
        TimeIntervalStrategy time_strategy(config_.control.time_interval, config_.target.timestamp_precision);
        std::cout << "\n=============================================== Insert Latency & Efficiency Metrics ==========================================\n"
                << "Total Operations: " << global_write_metrics.get_samples().size() << "\n"
                << "Total Duration: " << std::fixed << std::setprecision(2) << total_duration << " seconds\n"
                << "Pure Insert Latency: " << std::fixed << std::setprecision(2) << thread_latency << " seconds\n"
                << "Effective Time Ratio: " << std::fixed << std::setprecision(2) << effective_ratio << "%\n"
                << "Framework Overhead: " << std::fixed << std::setprecision(2) << framework_ratio << "%\n"
                << "Idle Time After Finish: " << std::fixed << std::setprecision(2) << avg_wait_time << " seconds\n";

        if (time_strategy.strategy_type() == IntervalStrategyType::Literal) {
            std::cout << "Play Latency Distribution: " << global_play_metrics.get_summary() << "\n";
        }

        std::cout << "Write Latency Distribution: " << global_write_metrics.get_summary() << "\n"
                << "==============================================================================================================================\n";

        std::cout << std::endl;

        // Clean up resources
        for (auto& writer : writers) {
            writer->close();
        }

        std::cout << "InsertDataAction completed successfully" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "InsertDataAction failed: " << e.what() << std::endl;
        throw;
    }
}

void InsertDataAction::producer_thread_function(
    size_t producer_id,
    const std::vector<std::string>& assigned_tables,
    const ColumnConfigInstanceVector& col_instances,
    DataPipeline<FormatResult>& pipeline,
    std::shared_ptr<TableDataManager> data_manager)
{
    // Initialize data manager
    if (!data_manager->init(assigned_tables)) {
        throw std::runtime_error("TableDataManager initialization failed for producer " + std::to_string(producer_id));
    }

    // Create formatter
    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config_.control.data_format);

    // Data generation loop
    while (auto batch = data_manager->next_multi_batch()) {
        // size_t batch_size = batch->table_batches.size();
        // size_t total_rows = batch->total_rows;

        // Format data
        FormatResult formatted_result = formatter->format(config_, col_instances, std::move(batch.value()));

        // Debug: print formatted result info
        // std::visit([producer_id](const auto& result) {
        //     using T = std::decay_t<decltype(result)>;
            
        //     if constexpr (std::is_same_v<T, SqlInsertData>) {
        //         std::cout << "Producer " << producer_id 
        //                   << ": sql data, rows: " << result.total_rows
        //                   << ", time range: [" << result.start_time 
        //                   << ", " << result.end_time << "]"
        //                   << ", length: " << result.data.str().length() 
        //                   << " bytes" << std::endl;
        //     } else if constexpr (std::is_same_v<T, StmtV2InsertData>) {
        //         std::cout << "Producer " << producer_id 
        //                   << ": stmt v2 data, rows: " << result.total_rows
        //                   << ", time range: [" << result.start_time 
        //                   << ", " << result.end_time << "]"
        //                 //   << ", length: " << result.data.length() 
        //                   << " bytes" << std::endl;
        //     } else if constexpr (std::is_same_v<T, std::string>) {
        //         std::cout << "Producer " << producer_id 
        //                   << ": unknown format result type: " 
        //                   << typeid(result).name() << ", content: " 
        //                   << result.substr(0, 100) 
        //                   << (result.length() > 100 ? "..." : "") 
        //                   << ", length: " << result.length() 
        //                   << " bytes" << std::endl;

        //         throw std::runtime_error("Unknown format result type: " + std::string(typeid(result).name()));
        //     }
        // }, formatted_result);

        // Push data to pipeline
        pipeline.push_data(producer_id, std::move(formatted_result));

        // std::cout << "Producer " << producer_id << ": Pushed batch for table(s): "
        //           << batch_size << ", total rows: " << total_rows 
        //           << ", queue size: " << pipeline.total_queued() << std::endl;
    }
}

void InsertDataAction::consumer_thread_function(
    size_t consumer_id,
    DataPipeline<FormatResult>& pipeline,
    std::atomic<bool>& running,
    IWriter* writer,
    GarbageCollector<FormatResult>& gc,
    Barrier& sync_barrier)
{
    // Failure retry logic
    const auto& failure_cfg = config_.control.insert_control.failure_handling;
    size_t retry_count = 0;
    

    sync_barrier.arrive_and_wait();

    // Data processing loop
    (void)running;
    while (true) {
        auto result = pipeline.fetch_data(consumer_id);

        switch (result.status) {
        case DataPipeline<FormatResult>::Status::Success:
            try {
                // Use writer to execute write
                std::visit([&](const auto& formatted_result) {
                    // using T = std::decay_t<decltype(formatted_result)>;
                    if constexpr (std::is_base_of_v<BaseInsertData, std::decay_t<decltype(formatted_result)>>) {
                        writer->write(formatted_result);
                        retry_count = 0;

                        // if constexpr (std::is_same_v<T, SqlInsertData> || std::is_same_v<T, StmtV2InsertData>) {
                        //     std::cout << "Consumer " << consumer_id 
                        //              << ": Executed SQL with " << formatted_result.total_rows 
                        //              << " rows" << std::endl;
                        // }

                    } else {
                        throw std::runtime_error("Unknown format result type: " + std::string(typeid(formatted_result).name()));
                    }

                }, *result.data);

                gc.dispose(std::move(*result.data));

            } catch (const std::exception& e) {
                std::cerr << "Consumer " << consumer_id << " write failed: " << e.what() << std::endl;
                
                // Handle failure
                if (retry_count < failure_cfg.max_retries) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(failure_cfg.retry_interval_ms));
                    retry_count++;
                } else if (failure_cfg.on_failure == "exit") {
                    // std::cerr << "Consumer " << consumer_id << " exiting after " 
                    //           << retry_count << " retries" << std::endl;
                    throw std::runtime_error(
                        "Consumer " + std::to_string(consumer_id) + 
                        " failed after " + std::to_string(retry_count) + 
                        " retries. Last error: " + e.what()
                    );
                    // return;
                }
            }
            break;
            
        case DataPipeline<FormatResult>::Status::Terminated:
            // Pipeline terminated, exit thread
            std::cout << "Consumer " << consumer_id << " received termination signal" << std::endl;
            return;
            
        case DataPipeline<FormatResult>::Status::Timeout:
            // Short sleep
            // std::this_thread::sleep_for(std::chrono::milliseconds(10));
            break;
        }
    }
}

ColumnConfigInstanceVector InsertDataAction::create_column_instances(const InsertDataConfig& config) const {
    try {
        const ColumnConfigVector& schema = [&]() -> const ColumnConfigVector& {
            if (config_.source.columns.source_type == "generator") {
                if (config_.source.columns.generator.schema.empty()) {
                    throw std::invalid_argument("Schema configuration is empty");
                }
                return config_.source.columns.generator.schema;
            } else if (config_.source.columns.source_type == "csv") {
                if (config_.source.columns.csv.schema.empty()) {
                    throw std::invalid_argument("CSV schema configuration is empty");
                }
                return config_.source.columns.csv.schema;
            }
            throw std::invalid_argument("Unsupported source type: " + config.source.columns.source_type);
        }();
        
        return ColumnConfigInstanceFactory::create(schema);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create column instances: ") + e.what());
    }
}

void InsertDataAction::print_writer_times(const std::vector<std::unique_ptr<IWriter>>& writers) {
    for (size_t i = 0; i < writers.size(); ++i) {
        auto start = writers[i]->start_write_time();
        auto end = writers[i]->end_write_time();

        auto start_sys = std::chrono::system_clock::now() + (start - std::chrono::steady_clock::now());
        auto end_sys = std::chrono::system_clock::now() + (end - std::chrono::steady_clock::now());

        auto print_time = [](const std::chrono::system_clock::time_point& tp) {
            auto t = std::chrono::system_clock::to_time_t(tp);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;
            std::tm tm;
#if defined(_WIN32)
            localtime_s(&tm, &t);
#else
            localtime_r(&t, &tm);
#endif
            std::cout << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
                      << "." << std::setfill('0') << std::setw(3) << ms.count();
        };

        std::cout << "[Debug] Writer " << i << " start_write_time: ";
        print_time(start_sys);
        std::cout << ", end_write_time: ";
        print_time(end_sys);
        std::cout << std::endl;
    }
}