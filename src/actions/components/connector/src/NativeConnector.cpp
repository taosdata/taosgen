#include "NativeConnector.h"
#include <iostream>

NativeConnector::NativeConnector(const ConnectionInfo& conn_info)
    : conn_info_(conn_info) {}

NativeConnector::~NativeConnector() {
    close();
}

bool NativeConnector::connect() {
    if (is_connected_) return true;

    conn_ = taos_connect(conn_info_.host.c_str(),
                         conn_info_.user.c_str(),
                         conn_info_.password.c_str(),
                         nullptr,
                         conn_info_.port);

    if (conn_ == nullptr) {
        std::cerr << "Native connection failed: " << taos_errstr(conn_) << std::endl;
        return false;
    }

    is_connected_ = true;
    return true;
}

bool NativeConnector::select_db(const std::string& db_name) {
    if (!is_connected_ && !connect()) return false;

    int code = taos_select_db(conn_, db_name.c_str());
    if (code != 0) {
        std::cerr << "Select database failed: " << taos_errstr(conn_) << std::endl;
        return false;
    }

    return true;
}

bool NativeConnector::prepare(const std::string& sql) {
    if (stmt_) {
        taos_stmt2_close(stmt_);
        stmt_ = nullptr;
    }

    // Create stmt option
    TAOS_STMT2_OPTION option = {};
    option.singleStbInsert = true;
    option.singleTableBindOnce = true;
    
    // init
    stmt_ = taos_stmt2_init(conn_, &option);
    if (!stmt_) {
        std::cerr << "Init stmt failed: " << taos_errstr(conn_) << std::endl;
        return false;
    }
    
    // prepare
    int code = taos_stmt2_prepare(stmt_, sql.c_str(), sql.size());
    if (code != 0) {
        std::cerr << "Prepare failed: " << taos_stmt2_error(stmt_) << ", SQL: " << sql << std::endl;
        taos_stmt2_close(stmt_);
        return false;
    }

    prepare_sql_ = sql;
    return true;
}

bool NativeConnector::execute(const std::string& sql) {
    if (!is_connected_ && !connect()) return false;

    TAOS_RES* res = taos_query(conn_, sql.c_str());
    const int code = taos_errno(res);
    const bool success = (code == 0);

    if (!success) {
        constexpr size_t max_sql_preview = 300;
        const bool is_truncated = sql.length() > max_sql_preview;
        
        std::cerr << "Native execute failed [" << code << "]: " 
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

bool NativeConnector::execute(const SqlInsertData& data) {
    return execute(data.data.str());
}

bool NativeConnector::execute(const StmtV2InsertData& data) {
    if (!is_connected_) return false;

    if (!stmt_) {
        std::cerr << "Statement is not prepared. Call prepare() first." << std::endl;
        return false;
    }

    // Bind data
    int code = taos_stmt2_bind_param(
        static_cast<TAOS_STMT*>(stmt_),
        const_cast<TAOS_STMT2_BINDV*>(data.data.bindv_ptr()),
        -1
    );
    if (code != 0) {
        std::cerr << "Failed to bind parameters: " 
                  << taos_stmt2_error(static_cast<TAOS_STMT*>(stmt_)) << std::endl;
        return false;
    }
    
    // Execute
    int affected_rows = 0;
    code = taos_stmt2_exec(stmt_, &affected_rows);
    if (code != 0) {
        std::cerr << "Execute failed: " << taos_stmt2_error(stmt_) << std::endl;
        return false;
    }

    return true;
}

void NativeConnector::close() noexcept {
    if (stmt_) {
        taos_stmt2_close(stmt_);
        stmt_ = nullptr;
    }
    if (conn_) {
        taos_close(conn_);
        conn_ = nullptr;
    }
    is_connected_ = false;
}