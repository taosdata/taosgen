#include "InsertDataAction.h"
#include <sched.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <variant>
#include <type_traits>
#include <pthread.h>
#include "FormatterRegistrar.h"
#include "FormatterFactory.h"
#include "TableNameManager.h"
#include "TableDataManager.h"
#include "WriterFactory.h"
#include "TimeRecorder.h"


void set_realtime_priority() {
    struct sched_param param;
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &param) != 0) {
        std::cerr << "Warning: Failed to set real-time priority. "
                  << "Requires root privileges or CAP_SYS_NICE capability.";
    }
}

void set_thread_affinity(size_t thread_id) {
    // Get available CPU cores
    unsigned int num_cores = std::thread::hardware_concurrency();
    if (num_cores == 0) num_cores = 1;
    
    // Calculate core ID, ensuring it's within valid range
    size_t core_id = thread_id % num_cores;
    
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
    
    std::cout << "Thread " << thread_id << " bound to core " << core_id << std::endl;
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
        // 1. 生成所有子表名并按生产者线程数切分
        TableNameManager name_manager(config_);
        auto all_names = name_manager.generate_table_names();
        
        // 校验生成的表名
        if (all_names.empty()) {
            throw std::runtime_error("No table names were generated");
        }
        
        auto split_names = name_manager.split_for_threads();
        
        // 校验切分结果
        if (split_names.size() != config_.control.data_generation.generate_threads) {
            throw std::runtime_error(
                "Split names count (" + std::to_string(split_names.size()) + 
                ") does not match generate_threads (" + 
                std::to_string(config_.control.data_generation.generate_threads) + ")"
            );
        }

        // 打印分配信息
        for (size_t i = 0; i < split_names.size(); i++) {
            std::cout << "Producer thread " << i << " will handle " 
                     << split_names[i].size() << " tables" << std::endl;
        }
        
        // 2. 创建列配置实例
        auto col_instances = create_column_instances(config_);

        // 3. 初始化数据管道
        const size_t producer_thread_count = config_.control.data_generation.generate_threads;
        const size_t consumer_thread_count = config_.control.insert_control.insert_threads;
        const size_t queue_capacity = config_.control.data_generation.queue_capacity;
        
        // 创建数据管道
        DataPipeline<FormatResult> pipeline(producer_thread_count, consumer_thread_count, queue_capacity);

        // 4. 启动消费者线程
        std::vector<std::thread> consumer_threads;
        consumer_threads.reserve(consumer_thread_count);

        std::vector<std::unique_ptr<IWriter>> writers;
        writers.reserve(consumer_thread_count);

        // 创建所有writer实例
        auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config_.control.data_format);
        auto sql = formatter->prepare(config_, col_instances, 1);
        for (size_t i = 0; i < consumer_thread_count; i++) {
            writers.push_back(WriterFactory::create(config_));
            
            // 连接数据库
            if (!writers[i]->connect()) {
                throw std::runtime_error("Failed to connect writer for thread " + std::to_string(i));
            }

            if (config_.target.target_type == "tdengine") {
                if (!writers[i]->select_db(config_.target.tdengine.database_info.name)) {
                    throw std::runtime_error("Failed to select database for writer thread " + std::to_string(i) + \
                        " with database name: " + config_.target.tdengine.database_info.name);  
                }
            }

            // 对于stmt v2格式，需要prepare
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

        // Start consumer threads
        for (size_t i = 0; i < consumer_thread_count; i++) {

            consumer_threads.emplace_back([this, i, &pipeline, &consumer_running, &writers] {
                // set_thread_affinity(i);

                set_realtime_priority();
                
                consumer_thread_function(i, pipeline, consumer_running[i], writers[i].get());
            });
        }

        const auto start_time = std::chrono::steady_clock::now();

        // 5. 启动生产者线程
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
            auto data_manager = std::make_shared<TableDataManager>(config_, col_instances);
            data_managers.push_back(data_manager);

            producer_threads.emplace_back([=, &pipeline, &active_producers, &producer_finished] {
                try {
                    producer_thread_function(i, split_names[i], col_instances, pipeline, data_manager);
                    producer_finished[i].store(true);
                } catch (const std::exception& e) {
                    std::cerr << "Producer thread " << i << " failed: " << e.what() << std::endl;
                }
                active_producers--;
            });
        }

        // 6. 监控
        // const auto start_time = std::chrono::steady_clock::now();
        size_t last_total_rows = 0;
        auto last_time = start_time;
        
        while (active_producers > 0) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            
            // 计算所有生产者生成的总行数
            size_t total_rows = 0;
            for (const auto& manager : data_managers) {
                total_rows += manager->get_total_rows_generated();
            }

            // 获取当前时间点
            const auto now = std::chrono::steady_clock::now();
            
            // 计算这次统计的实际时间间隔（秒）
            const auto interval = std::chrono::duration<double>(now - last_time).count();
            
            // 计算实时速率（行/秒）
            const double rows_per_sec = interval > 0 ? 
                static_cast<double>(total_rows - last_total_rows) / interval : 0.0;
            
            // 更新上次的值
            last_total_rows = total_rows;
            last_time = now;
            
            // 计算总运行时间
            const auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);
            
            std::cout << "Runtime: " << duration.count() << "s | "
                     << "Rate: " << std::fixed << std::setprecision(1) << rows_per_sec << " rows/s | "
                     << "Total: " << total_rows << " rows | "
                     << "Queue: " << pipeline.total_queued() << " items\n";
        }

        std::cout << "All producer threads have finished." << std::endl;
        // 等待所有所有数据被发送
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
                          << process_rate << " items/s" << std::endl;
                
                last_queue_size = current_queue_size;
                last_check_time = current_time;
            }
        }

        const auto end_time = std::chrono::steady_clock::now();

        // 计算最终的总行数
        size_t final_total_rows = 0;
        for (const auto& manager : data_managers) {
            final_total_rows += manager->get_total_rows_generated();
        }
        
        // 计算总耗时（秒）
        const auto total_duration = std::chrono::duration<double>(end_time - start_time).count();
        
        // 计算平均写入速率
        const double avg_rows_per_sec = total_duration > 0 ? 
            static_cast<double>(final_total_rows) / total_duration : 0.0;

        // 打印性能统计
        std::cout << "\n============================== Performance Statistics ===================================\n"
                  << "Insert Threads: " << consumer_thread_count << "\n"
                  << "Total Rows: " << final_total_rows << "\n"
                  << "Total Duration: " << std::fixed << std::setprecision(2) 
                  << total_duration << " seconds\n"
                  << "Average Rate: " << std::fixed << std::setprecision(2) 
                  << avg_rows_per_sec << " rows/second\n"
                  << "=========================================================================================\n\n";


        // 7. 等待生产者完成
        for (auto& t : producer_threads) {
            if (t.joinable()) t.join();
        }
        
        // 终止管道（通知消费者）
        pipeline.terminate();
        
        // 8. 等待消费者完成
        // for (size_t i = 0; i < consumer_thread_count; i++) {
        //     consumer_running[i] = false;
        // }
        
        for (auto& t : consumer_threads) {
            if (t.joinable()) t.join();
        }



        // 收集性能指标
        ActionMetrics global_metrics;
        std::vector<ActionMetrics> all_metrics;

        for (const auto& writer : writers) {
            all_metrics.push_back(writer->get_metrics());
        }

        // 合并所有指标
        global_metrics.merge_from(all_metrics);
        global_metrics.calculate();


        // 打印性能统计
        double thread_latency = global_metrics.get_sum() / consumer_thread_count / 1000;
        double effective_ratio = thread_latency / total_duration * 100.0;
        std::cout << "\n============================== Insert Performance Metrics ===============================\n"
                << "Total operations: " << global_metrics.get_samples().size() << "\n"
                << "Total duration: " << std::fixed << std::setprecision(2) << total_duration << " seconds\n"
                << "Pure insert latency: " << std::fixed << std::setprecision(2) << thread_latency << " seconds\n"
                << "Effective time ratio: " << std::fixed << std::setprecision(2) << effective_ratio << "%\n"
                << "Framework overhead: " << std::fixed << std::setprecision(2) << (100.0 - effective_ratio) << "%\n"
                << "Latency distribution: " << global_metrics.get_summary() << "\n"
                << "=========================================================================================\n\n";


        // 清理资源
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
    // 初始化数据管理器
    if (!data_manager->init(assigned_tables)) {
        throw std::runtime_error("TableDataManager initialization failed for producer " + std::to_string(producer_id));
    }

    // 创建格式化器
    auto formatter = FormatterFactory::instance().create_formatter<InsertDataConfig>(config_.control.data_format);

    // 数据生成循环
    while (auto batch = data_manager->next_multi_batch()) {
        // size_t batch_size = batch->table_batches.size();
        // size_t total_rows = batch->total_rows;

        // 格式化数据
        FormatResult formatted_result = formatter->format(config_, col_instances, std::move(batch.value()));

        // Debug: 打印格式化结果信息
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

        // 将数据推送到管道
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
    IWriter* writer)
{
    // 失败重试逻辑
    const auto& failure_cfg = config_.control.insert_control.failure_handling;
    size_t retry_count = 0;
    ActionMetrics metrics1_;
    ActionMetrics metrics2_;
    ActionMetrics metrics3_;
    ActionMetrics metrics4_;
    ActionMetrics metrics5_;
    ActionMetrics metrics6_;
    
    TimeRecorder timer6;
    // 数据处理循环
    (void)running;
    while (true) {
        TimeRecorder timer5;
        TimeRecorder timer1;
        auto result = pipeline.fetch_data(consumer_id);
        metrics1_.add_sample(timer1.elapsed());

        TimeRecorder timer4;
        switch (result.status) {
        case DataPipeline<FormatResult>::Status::Success:
        {
            TimeRecorder timer3;
            try {
                TimeRecorder timer2;
                // 使用writer执行写入
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
                metrics2_.add_sample(timer2.elapsed());

            } catch (const std::exception& e) {
                std::cerr << "Consumer " << consumer_id << " write failed: " << e.what() << std::endl;
                
                // 处理失败
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
            metrics3_.add_sample(timer3.elapsed());
            break;
        }
        case DataPipeline<FormatResult>::Status::Terminated:
            // 管道已终止，退出线程
            metrics4_.add_sample(timer4.elapsed());
            metrics5_.add_sample(timer5.elapsed());
            metrics6_.add_sample(timer6.elapsed());

            metrics1_.calculate();
            metrics2_.calculate();
            metrics3_.calculate();
            metrics4_.calculate();
            metrics5_.calculate();
            metrics6_.calculate();


            // 打印性能统计
            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " GetData Performance Metrics ===============================\n"
                    << "Total operations: " << metrics1_.get_samples().size() << "\n"
                    << "Latency: " << metrics1_.get_summary() << "\n"
                    << "=========================================================================================\n\n";

            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " Visit Performance Metrics ===============================\n"
                    << "Total operations: " << metrics2_.get_samples().size() << "\n"
                    << "Latency: " << metrics2_.get_summary() << "\n"
                    << "=========================================================================================\n\n";

            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " Pipeline Performance Metrics ===============================\n"
                    << "Total operations: " << metrics3_.get_samples().size() << "\n"
                    << "Latency: " << metrics3_.get_summary() << "\n"
                    << "=========================================================================================\n\n";

            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " Switch Performance Metrics ===============================\n"
                    << "Total operations: " << metrics4_.get_samples().size() << "\n"
                    << "Latency: " << metrics4_.get_summary() << "\n"
                    << "=========================================================================================\n\n";


            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " While Performance Metrics ===============================\n"
                    << "Total operations: " << metrics5_.get_samples().size() << "\n"
                    << "Latency: " << metrics5_.get_summary() << "\n"
                    << "=========================================================================================\n\n";


            std::cout << "\n============================== Insert " << "Consumer " << consumer_id
                    << " Callback Performance Metrics ===============================\n"
                    << "Total operations: " << metrics6_.get_samples().size() << "\n"
                    << "Latency: " << metrics6_.get_summary() << "\n"
                    << "=========================================================================================\n\n";


            std::cout << "Consumer " << consumer_id << " received termination signal" << std::endl;
            return;
            
        case DataPipeline<FormatResult>::Status::Timeout:
            {
                // 获取并打印当前时间，精确到毫秒
                auto now = std::chrono::system_clock::now();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
                auto now_t = std::chrono::system_clock::to_time_t(now);
                
                std::stringstream ss;
                ss << std::put_time(std::localtime(&now_t), "%Y-%m-%d %H:%M:%S")
                   << '.' << std::setfill('0') << std::setw(3) 
                   << now_ms.count() % 1000;
                    
                std::cout << "Consumer " << consumer_id 
                          << " sleeping at: " << ss.str() << std::endl;
                        
                // 短暂休眠
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        metrics4_.add_sample(timer4.elapsed());
        metrics5_.add_sample(timer5.elapsed());
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
