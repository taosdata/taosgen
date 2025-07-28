#pragma once

#include <memory>
#include <stdexcept>
#include <atomic>
#include "blockingconcurrentqueue.h"

template <typename T>
class DataPipeline {
public:
    enum class Status {
        Success,
        Timeout,
        Terminated
    };

    struct Result {
        std::unique_ptr<T> data;
        Status status;
    };

    DataPipeline(size_t queue_capacity = 100) : queue_(queue_capacity) {}

    ~DataPipeline() {
        terminate();
    }

    DataPipeline(const DataPipeline&) = delete;
    DataPipeline& operator=(const DataPipeline&) = delete;

    void push_data(T formatted_data) {
        if (terminated_.load(std::memory_order_acquire)) {
            throw std::runtime_error("Pipeline has been terminated");
        }
        queue_.enqueue(std::make_unique<T>(std::move(formatted_data)));
    }

    Result fetch_data() {
        std::unique_ptr<T> item;
        if (queue_.wait_dequeue_timed(item, std::chrono::milliseconds(100))) {
            if (item) {
                return Result{std::move(item), Status::Success};
            } else {
                queue_.enqueue(nullptr);
                return Result{nullptr, Status::Terminated};
            }
        } else {
            return terminated_.load(std::memory_order_acquire)
                ? Result{nullptr, Status::Terminated}
                : Result{nullptr, Status::Timeout};
        }
    }

    void terminate() {
        terminated_.store(true, std::memory_order_release);
        queue_.enqueue(nullptr);
    }
    
    size_t total_queued() const {
        return queue_.size_approx();
    }

private:
    std::atomic<bool> terminated_{false};
    moodycamel::BlockingConcurrentQueue<std::unique_ptr<T>> queue_;
};