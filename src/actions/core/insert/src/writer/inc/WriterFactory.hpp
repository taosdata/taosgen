#pragma once
#include "IWriter.hpp"
#include "InsertDataConfig.hpp"
#include "ActionRegisterInfo.hpp"
#include <memory>
#include <stdexcept>

class WriterFactory {
public:

    using Builder = std::function<std::unique_ptr<IWriter>(const InsertDataConfig&,
                                                           size_t,
                                                           std::shared_ptr<ActionRegisterInfo>)>;

    static WriterFactory& instance() {
        static WriterFactory inst;
        return inst;
    }

    static void register_writer(const std::string& target, Builder builder) {
        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        inst.writers_[target] = std::move(builder);
    }

    static std::unique_ptr<IWriter> create_writer(const InsertDataConfig& config,
                                                  size_t no = 0,
                                                  std::shared_ptr<ActionRegisterInfo> action_info = nullptr) {

        auto& inst = instance();
        std::lock_guard<std::mutex> lock(inst.mutex_);
        if (auto it = inst.writers_.find(config.target_type); it != inst.writers_.end()) {
            return it->second(config, no, std::move(action_info));
        }

        throw std::invalid_argument("Unsupported target type: " + config.target_type);
    }

private:
    std::unordered_map<std::string, Builder> writers_;
    std::mutex mutex_;
};