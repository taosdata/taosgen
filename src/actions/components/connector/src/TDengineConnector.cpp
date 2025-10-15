#include "TDengineConnector.hpp"
#include <iostream>
#include <sstream>
#include <stdexcept>

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
#if defined(_WIN32)
    taos_lib_handle_ = DYNLIB_LOAD("taos.dll");
#elif defined(__APPLE__)
    taos_lib_handle_ = DYNLIB_LOAD("libtaos.dylib");
#else
    taos_lib_handle_ = DYNLIB_LOAD("libtaos.so");
#endif

    if (!taos_lib_handle_) {
        throw std::runtime_error("Failed to load libtaos shared library");
    }

    // Load API
    taos_options_ = reinterpret_cast<taos_options_func>(DYNLIB_SYM(taos_lib_handle_, "taos_options"));
    taos_errstr_ = reinterpret_cast<taos_errstr_func>(DYNLIB_SYM(taos_lib_handle_, "taos_errstr"));
    taos_errno_ = reinterpret_cast<taos_errno_func>(DYNLIB_SYM(taos_lib_handle_, "taos_errno"));
    taos_connect_ = reinterpret_cast<taos_connect_func>(DYNLIB_SYM(taos_lib_handle_, "taos_connect"));
    taos_query_ = reinterpret_cast<taos_query_func>(DYNLIB_SYM(taos_lib_handle_, "taos_query"));
    taos_free_result_ = reinterpret_cast<taos_free_result_func>(DYNLIB_SYM(taos_lib_handle_, "taos_free_result"));
    taos_select_db_ = reinterpret_cast<taos_select_db_func>(DYNLIB_SYM(taos_lib_handle_, "taos_select_db"));
    taos_close_ = reinterpret_cast<taos_close_func>(DYNLIB_SYM(taos_lib_handle_, "taos_close"));

    taos_stmt2_init_ = reinterpret_cast<taos_stmt2_init_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_init"));
    taos_stmt2_prepare_ = reinterpret_cast<taos_stmt2_prepare_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_prepare"));
    taos_stmt2_bind_param_ = reinterpret_cast<taos_stmt2_bind_param_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_bind_param"));
    taos_stmt2_exec_ = reinterpret_cast<taos_stmt2_exec_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_exec"));
    taos_stmt2_close_ = reinterpret_cast<taos_stmt2_close_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_close"));
    taos_stmt2_error_ = reinterpret_cast<taos_stmt2_error_func>(DYNLIB_SYM(taos_lib_handle_, "taos_stmt2_error"));

    // Check if the loading was successful
    if (!taos_options_ || !taos_errstr_ || !taos_errno_ || !taos_connect_ ||
        !taos_query_ || !taos_free_result_ || !taos_select_db_ || !taos_close_ ||
        !taos_stmt2_init_ || !taos_stmt2_prepare_ || !taos_stmt2_bind_param_ ||
        !taos_stmt2_exec_ || !taos_stmt2_close_ || !taos_stmt2_error_)
    {
        DYNLIB_CLOSE(taos_lib_handle_);
        taos_lib_handle_ = nullptr;
        throw std::runtime_error("Failed to load all required taos API symbols");
    }


    std::lock_guard<std::mutex> lock(driver_init_mutex_);
    auto& flag = driver_init_flags[driver_type_];

    std::call_once(flag, [this]() {
        // Setting driver
        int32_t code = taos_options_(TSDB_OPTION_DRIVER, driver_type_.c_str());
        if (code != 0) {
            std::ostringstream oss;
            oss << "Failed to set driver to " << display_name_ << ": "
                << taos_errstr_(nullptr) << " [0x"
                << std::hex << taos_errno_(nullptr) << "]";
            throw std::runtime_error(oss.str());
        }
    });
}

TDengineConnector::~TDengineConnector() {
    close();
    if (taos_lib_handle_) {
        DYNLIB_CLOSE(taos_lib_handle_);
        taos_lib_handle_ = nullptr;
    }
}

bool TDengineConnector::connect() {
    if (is_connected_) return true;
    init_driver();

    conn_ = taos_connect_(
        conn_info_.host.c_str(),
        conn_info_.user.c_str(),
        conn_info_.password.c_str(),
        nullptr,
        conn_info_.port
    );

    if (!conn_) {
        std::cerr << display_name_ << " connection failed: "
                  << taos_errstr_(nullptr)
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

    TAOS_RES* res = taos_query_(conn_, "SELECT 1");
    if (taos_errno_(res) != 0) {
        taos_free_result_(res);
        return false;
    }
    taos_free_result_(res);
    return true;
}

bool TDengineConnector::select_db(const std::string& db_name) {
    if (!is_connected_ && !connect()) return false;

    int code = taos_select_db_(conn_, db_name.c_str());
    if (code != 0) {
        std::cerr << display_name_ << " select database failed: "
                  << taos_errstr_(conn_) << std::endl;
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
        taos_stmt2_close_(stmt_);
        stmt_ = nullptr;
    }

    // Create stmt option
    TAOS_STMT2_OPTION option = {};
    option.singleStbInsert = true;
    option.singleTableBindOnce = true;

    // Init
    stmt_ = taos_stmt2_init_(conn_, &option);
    if (!stmt_) {
        std::cerr << display_name_ << " init stmt failed: "
                  << taos_errstr_(conn_) << std::endl;
        return false;
    }

    // Prepare
    int code = taos_stmt2_prepare_(stmt_, sql.c_str(), sql.size());
    if (code != 0) {
        std::cerr << display_name_ << " prepare failed: "
                  << taos_stmt2_error_(stmt_) << ", SQL: " << sql << std::endl;
        taos_stmt2_close_(stmt_);
        stmt_ = nullptr;
        return false;
    }

    prepare_sql_ = sql;
    return true;
}

bool TDengineConnector::execute(const std::string& sql) {
    if (!is_connected_ && !connect()) return false;

    TAOS_RES* res = taos_query_(conn_, sql.c_str());
    const int code = taos_errno_(res);
    const bool success = (code == 0);

    if (!success) {
        constexpr size_t max_sql_preview = 300;
        const bool is_truncated = sql.length() > max_sql_preview;

        std::cerr << display_name_ << " execute failed [" << code << "]: "
                  << taos_errstr_(res) << "\n"
                  << (is_truncated ? "SQL (truncated): " : "SQL: ")
                  << sql.substr(0, max_sql_preview)
                  << (is_truncated ? "..." : "")
                  << ", SQL length: " << sql.length() << " bytes"
                  << std::endl;
    }

    taos_free_result_(res);
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
    int code = taos_stmt2_bind_param_(
        stmt_,
        const_cast<TAOS_STMT2_BINDV*>(data.bindv_ptr()),
        -1
    );
    if (code != 0) {
        std::cerr << display_name_ << " failed to bind parameters: "
                  << taos_stmt2_error_(stmt_)
                  << " [code: 0x" << std::hex << code << std::dec << "]"
                  << std::endl;
        return false;
    }

    // Execute
    int affected_rows = 0;
    code = taos_stmt2_exec_(stmt_, &affected_rows);
    if (code != 0) {
        std::cerr << display_name_ << " execute failed: "
                  << taos_stmt2_error_(stmt_)
                  << " [code: 0x" << std::hex << code << std::dec << "]"
                  << std::endl;
        return false;
    }

    return true;
}

void TDengineConnector::reset_state() noexcept {
    if (stmt_) {
        taos_stmt2_close_(stmt_);
        stmt_ = nullptr;
    }
    current_db_.clear();
}

void TDengineConnector::close() noexcept {
    reset_state();
    if (conn_) {
        taos_close_(conn_);
        conn_ = nullptr;
    }
    is_connected_ = false;
}