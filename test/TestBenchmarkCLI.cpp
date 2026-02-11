#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static std::string find_taosgen_bin() {
    if (const char* env = std::getenv("TAOSGEN_BIN")) {
        if (env[0] != '\0' && fs::exists(env)) {
            return fs::absolute(env).string();
        }
    }

    std::vector<std::string> candidates = {
        "./src/taosgen",
        "../src/taosgen",
        "./taosgen",
        "src/taosgen",
        "../build/src/taosgen"
    };

    for (const auto& c : candidates) {
        if (fs::exists(c)) {
            return fs::absolute(fs::path(c)).string();
        }
    }

    return {};
}

static std::string find_project_root() {
    if (const char* env = std::getenv("TSGEN_ROOT")) {
        if (env[0] != '\0' && fs::exists(fs::path(env) / "src/benchmark.cpp")) {
            return env;
        }
    }

    fs::path p = fs::current_path();
    for (int i = 0; i < 6; ++i) {
        if (fs::exists(p / "src/benchmark.cpp")) {
            return p.string();
        }
        if (p.has_parent_path()) {
            p = p.parent_path();
        } else {
            break;
        }
    }
    return {};
}

static std::string shell_cd(const std::string& dir) {
#if defined(_WIN32)
    return "cd /d \"" + dir + "\" && ";
#else
    return "cd \"" + dir + "\" && ";
#endif
}

static int run_cmd(const std::string& cmd) {
    std::cout << "[RUN] " << cmd << std::endl;
    return std::system(cmd.c_str());
}

void test_help_command() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

    const std::string cmd = "\"" + bin + "\" --help";
    int rc = run_cmd(cmd);
    (void)rc;
    assert(rc == 0 && "taosgen --help should exit 0");
    std::cout << "test_help_command passed\n";
}

void test_config_command() {
    const auto bin = find_taosgen_bin();
    assert(!bin.empty() && "taosgen binary not found; set TAOSGEN_BIN");

    const auto root = find_project_root();
    assert(!root.empty() && "project root not found; set TSGEN_ROOT");

    const fs::path bin_path = fs::path(bin);
    const fs::path build_dir = bin_path.parent_path().parent_path();

    const std::string cfg = (fs::path(root) / "conf/tdengine-csv.yaml").string();
    const std::string cmd = shell_cd(build_dir.string()) + "\"" + bin + "\" -v -c \"" + cfg + "\"";
    int rc = run_cmd(cmd);
    (void)rc;
    assert(rc == 0 && "taosgen -c <root>/conf/tdengine-csv.yaml should exit 0");
    std::cout << "test_config_command passed\n";
}

int main() {
    test_help_command();
    test_config_command();
    std::cout << "All Benchmark command tests passed.\n";
    return 0;
}