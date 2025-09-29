#include "TDengineConnector.hpp"
#include <iostream>
#include <sstream>

std::mutex TDengineConnector::driver_init_mutex_;
std::map<std::string, std::once_flag> TDengineConnector::driver_init_flags;

TDengineConnector::TDengineConnector(const TDengineConfig& conn_info,
                                             const std::string& driver_type,
                                             const std::string& display_name)
    : conn_info_(conn_info),
      driver_type_(driver_type),
      display_name_(display_name) {
    init_driver();
}

void TDengineConnector::init_driver() {
    std::lock_guard<std::mutex> lock(driver_init_mutex_);
    auto& flag = driver_init_flags[driver_type_];

    std::call_once(flag, [this]() {
        int32_t code = taos_options(TSDB_OPTION_DRIVER, driver_type_.c_str());
        if (code != 0) {
            std::ostringstream oss;
            oss << "Failed to set driver to " << display_name_ << ": "
                << taos_errstr(nullptr) << " [0x"
                << std::hex << taos_errno(nullptr) << "]";
            throw std::runtime_error(oss.str());
        }
    });
}

TDengineConnector::~TDengineConnector() {
    close();
}

bool TDengineConnector::connect() {
    if (is_connected_) return true;

    conn_ = taos_connect(conn_info_.host.c_str(),
                         conn_info_.user.c_str(),
                         conn_info_.password.c_str(),
                         nullptr,
                         conn_info_.port);

    if (!conn_) {
        std::cerr << display_name_ << " connection failed: "
                  << taos_errstr(nullptr)
                  << " [host: " << conn_info_.host
                  << ", port: " << conn_info_.port << "]"
                  << std::endl;
        return false;
    }

    is_connected_ = true;
    return true;
}

bool TDengineConnector::is_connected() const {
    return is_connected_;
}

bool TDengineConnector::is_valid() const {
    if (!is_connected_) return false;

    TAOS_RES* res = taos_query(conn_, "SELECT 1");
    if (taos_errno(res) != 0) {
        taos_free_result(res);
        return false;
    }
    taos_free_result(res);
    return true;
}

bool TDengineConnector::select_db(const std::string& db_name) {
    if (!is_connected_ && !connect()) return false;

    int code = taos_select_db(conn_, db_name.c_str());
    if (code != 0) {
        std::cerr << display_name_ << " select database failed: "
                  << taos_errstr(conn_) << std::endl;
        return false;
    }

    current_db_ = db_name;

    return true;
}

bool TDengineConnector::prepare(const std::string& sql) {
    if (sql.empty()) {
        return true;
    }

    if (stmt_) {
        taos_stmt2_close(stmt_);
        stmt_ = nullptr;
    }

    // Create stmt option
    TAOS_STMT2_OPTION option = {};
    option.singleStbInsert = true;
    option.singleTableBindOnce = true;

    // Init
    stmt_ = taos_stmt2_init(conn_, &option);
    if (!stmt_) {
        std::cerr << display_name_ << " init stmt failed: "
                  << taos_errstr(conn_) << std::endl;
        return false;
    }

    // Prepare
    int code = taos_stmt2_prepare(stmt_, sql.c_str(), sql.size());
    if (code != 0) {
        std::cerr << display_name_ << " prepare failed: "
                  << taos_stmt2_error(stmt_) << ", SQL: " << sql << std::endl;
        taos_stmt2_close(stmt_);
        stmt_ = nullptr;
        return false;
    }

    prepare_sql_ = sql;
    return true;
}

bool TDengineConnector::execute(const std::string& sql) {
    if (!is_connected_ && !connect()) return false;

    TAOS_RES* res = taos_query(conn_, sql.c_str());
    const int code = taos_errno(res);
    const bool success = (code == 0);

    if (!success) {
        constexpr size_t max_sql_preview = 300;
        const bool is_truncated = sql.length() > max_sql_preview;

        std::cerr << display_name_ << " execute failed [" << code << "]: "
                  << taos_errstr(res) << "\n"
                  << (is_truncated ? "SQL (truncated): " : "SQL: ")
                  << sql.substr(0, max_sql_preview)
                  << (is_truncated ? "..." : "")
                  << ", SQL length: " << sql.length() << " bytes"
                  << std::endl;
    }

    taos_free_result(res);
    return success;
}

bool TDengineConnector::execute(const SqlInsertData& data) {
    return execute(data.data.str());
}

bool TDengineConnector::execute(const StmtV2InsertData& data) {
    if (!is_connected_) return false;

    if (!stmt_) {
        std::cerr << display_name_ << " statement is not prepared. Call prepare() first." << std::endl;
        return false;
    }

    // Bind data
    int code = taos_stmt2_bind_param(
        stmt_,
        const_cast<TAOS_STMT2_BINDV*>(data.bindv_ptr()),
        -1
    );
    if (code != 0) {
        std::cerr << display_name_ << " failed to bind parameters: "
                  << taos_stmt2_error(stmt_)
                  << " [code: 0x" << std::hex << code << std::dec << "]"
                  << std::endl;
        return false;
    }

    // Execute
    int affected_rows = 0;
    code = taos_stmt2_exec(stmt_, &affected_rows);
    if (code != 0) {
        std::cerr << display_name_ << " execute failed: "
                  << taos_stmt2_error(stmt_)
                  << " [code: 0x" << std::hex << code << std::dec << "]"
                  << std::endl;
        return false;
    }

    return true;
}

void TDengineConnector::reset_state() noexcept {
    if (stmt_) {
        taos_stmt2_close(stmt_);
        stmt_ = nullptr;
    }
    current_db_.clear();
}

void TDengineConnector::close() noexcept {
    reset_state();
    if (conn_) {
        taos_close(conn_);
        conn_ = nullptr;
    }
    is_connected_ = false;
}