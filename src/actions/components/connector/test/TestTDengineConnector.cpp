#include "TDengineConnector.hpp"
#include "ProcessUtils.hpp"
#include <iostream>
#include <filesystem>
#include <stdexcept>
#include <cassert>

#if defined(_WIN32)
static constexpr const char* kLibName = "taos.dll";
static constexpr char kSep = '\\';
#else
#  if defined(__APPLE__)
static constexpr const char* kLibName = "libtaos.dylib";
#  else
static constexpr const char* kLibName = "libtaos.so";
#  endif
static constexpr char kSep = '/';
#endif

static std::string program_lib_dir() {
    std::string dir = ProcessUtils::get_exe_directory();
    if (!dir.empty() && dir.back() != kSep) dir.push_back(kSep);
    dir += "lib";
    return dir;
}

static std::string program_lib_path() {
    auto dir = program_lib_dir();
    std::filesystem::path p(dir);
    p /= kLibName;
    return p.string();
}

#if !defined(_WIN32)
static std::filesystem::path find_system_lib_candidate() {
    const char* candidates[] = { "/usr/local/lib", "/usr/lib" };
    for (auto* base : candidates) {
        std::filesystem::path p = std::filesystem::path(base) / kLibName;
        if (std::filesystem::exists(p)) return p;
    }
    return {};
}
#endif

static bool g_prog_lib_created_by_test = false;

static bool ensure_program_lib_present() {
    const std::string lib_dir = program_lib_dir();
    const std::string full = program_lib_path();

    std::error_code ec;
    std::filesystem::create_directories(lib_dir, ec);
    if (ec) {
        std::cerr << "[setup] FAIL: create lib dir failed: " << lib_dir << " (" << ec.message() << ")\n";
        assert(false && "Failed to create program lib directory");
        return false;
    }

    assert(std::filesystem::exists(lib_dir));
    assert(std::filesystem::is_directory(lib_dir));

    if (std::filesystem::exists(full)) {
        std::cout << "[setup] lib already present: " << full << "\n";
        g_prog_lib_created_by_test = false;
        assert(std::filesystem::path(full).filename().string() == kLibName);
        return true;
    }

#if defined(_WIN32)
    std::cerr << "[setup] On Windows, place " << kLibName << " under " << lib_dir << " manually\n";
    assert(false && "Program lib missing on Windows for this test");
    return false;
#else
    auto sys_lib = find_system_lib_candidate();
    if (sys_lib.empty()) {
        std::cerr << "[setup] System lib not found in /usr/local/lib or /usr/lib\n";
        assert(false && "System lib not found; cannot set up prefer path");
        return false;
    }

    std::error_code ec2;
    std::filesystem::copy_file(sys_lib, full, std::filesystem::copy_options::overwrite_existing, ec2);
    if (ec2) {
        std::cerr << "[setup] FAIL: copy " << sys_lib << " -> " << full << " (" << ec2.message() << ")\n";
        assert(false && "Failed to copy system lib to program lib dir");
        return false;
    }
    std::cout << "[setup] Copied " << sys_lib << " -> " << full << "\n";
    g_prog_lib_created_by_test = true;

    assert(std::filesystem::exists(full));
    assert(std::filesystem::path(full).filename().string() == kLibName);
    return true;
#endif
}

static void cleanup_program_lib_if_created() {
#if !defined(_WIN32)
    if (!g_prog_lib_created_by_test) return;
    const std::string full = program_lib_path();
    const std::string dir = program_lib_dir();

    std::error_code ec;
    if (std::filesystem::exists(full)) {
        std::filesystem::remove(full, ec);
        if (ec) {
            std::cerr << "[cleanup] WARN: failed to remove " << full << " (" << ec.message() << ")\n";
            assert(false && "Failed to remove test-copied lib");
        } else {
            std::cout << "[cleanup] Removed test-copied lib: " << full << "\n";
        }
    }

    std::error_code ec2;
    if (std::filesystem::exists(dir) && std::filesystem::is_directory(dir) && std::filesystem::is_empty(dir)) {
        std::filesystem::remove(dir, ec2);
        if (ec2) {
            std::cerr << "[cleanup] WARN: failed to remove empty dir " << dir << " (" << ec2.message() << ")\n";
            assert(false && "Failed to remove empty lib dir");
        } else {
            std::cout << "[cleanup] Removed empty lib dir: " << dir << "\n";
        }
    }
    g_prog_lib_created_by_test = false;
#endif
}

static void try_construct_connector_expect_loaded(const char* case_name) {
    TDengineConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.user.clear();
    cfg.password.clear();
    cfg.database.clear();

    const std::string driver = "native";
    bool ok = false;
    try {
        TDengineConnector conn(cfg, driver, "TDengine");
        std::cout << "[" << case_name << "] OK: library loaded and driver set: " << driver << std::endl;
        ok = true;
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        if (msg.find("Failed to load libtaos shared library") != std::string::npos) {
            std::cerr << "[" << case_name << "] FAIL: library load failed: " << msg << std::endl;
            assert(false && "Failed to load libtaos shared library");
        } else {
            std::cerr << "[" << case_name << "] FAIL: unexpected runtime error: " << msg << std::endl;
            assert(false && "Unexpected runtime error during connector init");
        }
    } catch (...) {
        std::cerr << "[" << case_name << "] FAIL: unexpected exception." << std::endl;
        assert(false && "Unexpected exception");
    }
    (void)ok;
    assert(ok && "Connector should initialize without exception for native driver");
}

static void test_prefer_program_dir_lib() {
    bool ensured = ensure_program_lib_present();
    (void)ensured;
    assert(ensured && "Program lib must be ensured for prefer test");

    assert(std::filesystem::exists(program_lib_path()));
    assert(std::filesystem::path(program_lib_path()).filename().string() == kLibName);

    std::cout << "[prefer] Trying to load from: " << program_lib_path() << std::endl;
    try_construct_connector_expect_loaded("prefer");
}

static void test_fallback_system_path() {
#if defined(_WIN32)
    std::cerr << "[fallback] Not supported on Windows in this test\n";
    assert(false && "Fallback test not supported on Windows");
#else
    const auto sys_lib = find_system_lib_candidate();
    if (sys_lib.empty()) {
        std::cerr << "[fallback] FAIL: no system lib found to fallback\n";
        assert(false && "System lib missing; cannot test fallback");
    }

    const std::filesystem::path prog_lib(program_lib_path());
    bool temporarily_moved = false;
    std::filesystem::path backup;

    if (std::filesystem::exists(prog_lib)) {
        backup = prog_lib;
        backup += ".bak";
        std::error_code ec_rm, ec_mv;
        std::filesystem::remove(backup, ec_rm);
        std::filesystem::rename(prog_lib, backup, ec_mv);
        if (!ec_mv) {
            temporarily_moved = true;
            std::cout << "[fallback] Temporarily moved: " << prog_lib << " -> " << backup << std::endl;
            assert(!std::filesystem::exists(prog_lib));
        } else {
            std::cerr << "[fallback] FAIL: could not move program lib (" << ec_mv.message() << ")\n";
            assert(false && "Failed to move program lib for fallback test");
        }
    } else {
        std::cout << "[fallback] Program lib absent, testing system path fallback\n";
    }

    try_construct_connector_expect_loaded("fallback");

    if (temporarily_moved) {
        std::error_code ec2;
        std::filesystem::rename(backup, prog_lib, ec2);
        if (ec2) {
            std::cerr << "[fallback] WARN: failed to restore " << backup << " -> " << prog_lib << " (" << ec2.message() << ")\n";
            assert(false && "Failed to restore program lib after fallback test");
        } else {
            std::cout << "[fallback] Restored: " << backup << " -> " << prog_lib << std::endl;
            assert(std::filesystem::exists(prog_lib));
        }
    }
#endif
}

int main() {
    std::cout << "Running TDengineConnector library loading tests..." << std::endl;

    test_prefer_program_dir_lib();
    test_fallback_system_path();

    cleanup_program_lib_if_created();

    std::cout << "TDengineConnector library loading tests completed." << std::endl;
    return 0;
}
