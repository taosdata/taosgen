#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>
#include <functional>
#include <memory>
#include <stdexcept>


template <typename T>
class GarbageCollector {
public:
    struct Task {
        std::function<void()> destructor;

        Task() = default;
        Task(Task&&) = default;
        Task& operator=(Task&&) = default;
    };

    GarbageCollector(size_t writer_threads, size_t group_size = 1)
        : terminated_(false),
          writer_threads_(writer_threads),
          group_size_(group_size) {
        
        if (writer_threads == 0) throw std::invalid_argument("writer_threads cannot be zero");
        if (group_size == 0) throw std::invalid_argument("group_size cannot be zero");
        
        // 计算回收线程组数
        size_t groups = (writer_threads + group_size - 1) / group_size;
        queues_.resize(groups);
        threads_.reserve(groups);

        // 启动回收线程
        for (size_t i = 0; i < groups; ++i) {
            threads_.emplace_back([this, i] { worker_thread(i); });
        }
    }

    ~GarbageCollector() {
        terminate();
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
    }

    // 添加待销毁的数据对象
    void dispose(size_t writer_id, T&& data) {
        if (terminated_) return;
        
        // 计算该写入线程对应的回收队列索引
        size_t gc_index = writer_id % queues_.size();
        auto& queue = queues_[gc_index];
        
        {
            std::lock_guard<std::mutex> lock(queue.mutex);
            
            // 创建销毁任务
            Task task;
            task.destructor = [ptr = std::make_shared<std::decay_t<T>>(std::move(data))]() {

            };
            queue.tasks.push(std::move(task));
        }
        queue.cv.notify_one();
    }

    void terminate() {
        terminated_.store(true);
        for (auto& queue : queues_) {
            std::lock_guard<std::mutex> lock(queue.mutex);
            queue.cv.notify_all();
        }
    }

private:
    void worker_thread(size_t queue_index) {
        auto& queue = queues_[queue_index];
        
        while (!terminated_) {
            Task task;
            
            {
                std::unique_lock<std::mutex> lock(queue.mutex);
                queue.cv.wait(lock, [&] {
                    return !queue.tasks.empty() || terminated_;
                });
                
                if (terminated_ && queue.tasks.empty()) break;
                
                if (!queue.tasks.empty()) {
                    task = std::move(queue.tasks.front());
                    queue.tasks.pop();
                }
            }
            
            // 执行析构任务
            if (task.destructor) {
                task.destructor();
            }
        }
        
        // 清空剩余任务
        std::queue<Task> remaining;
        {
            std::lock_guard<std::mutex> lock(queue.mutex);
            remaining = std::move(queue.tasks);
        }
        
        while (!remaining.empty()) {
            if (auto& task = remaining.front(); task.destructor) {
                task.destructor();
            }
            remaining.pop();
        }
    }

    struct GCQueue {
        std::queue<Task> tasks;
        mutable std::mutex mutex;
        std::condition_variable cv;
        
        // 支持移动语义
        GCQueue() = default;
        GCQueue(GCQueue&& other) noexcept 
            : tasks(std::move(other.tasks)) 
        {}
    };

    std::vector<GCQueue> queues_;
    std::vector<std::thread> threads_;
    std::atomic<bool> terminated_;
    const size_t writer_threads_;
    const size_t group_size_;
};