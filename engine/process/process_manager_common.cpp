// ============================================================================
// process_manager_common.cpp - platform-neutral part of process control
// (pid files, project-root discovery, peer executable lookup)
// ============================================================================
#include "process_manager.hpp"

#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace proc {

std::string pid_file_for(const std::string& id) { return "run/peer_" + id + ".pid"; }
std::string log_file_for(const std::string& id) { return "run/peer_" + id + ".log"; }

long read_pid_file(const std::string& path) {
    std::ifstream in(path);
    long pid = 0;
    if (!(in >> pid) || pid < 0) return 0;
    return pid;
}

bool write_pid_file(const std::string& path, long pid) {
    std::error_code ec;
    fs::create_directories(fs::path(path).parent_path(), ec);
    std::ofstream out(path, std::ios::trunc);
    out << pid << "\n";
    return static_cast<bool>(out);
}

void remove_pid_file(const std::string& path) {
    std::error_code ec;
    fs::remove(path, ec);
}

namespace {
bool has_config(const fs::path& dir) {
    std::error_code ec;
    return fs::is_regular_file(dir / "config.json", ec) ||
           fs::is_regular_file(dir / "config" / "config.json", ec);
}

bool search_upwards(fs::path dir, fs::path& found) {
    for (int i = 0; i < 8 && !dir.empty(); ++i) {
        if (has_config(dir)) {
            found = dir;
            return true;
        }
        fs::path parent = dir.parent_path();
        if (parent == dir) break;
        dir = parent;
    }
    return false;
}
}  // namespace

bool enter_project_root(std::string& root_out, std::string& err) {
    std::error_code ec;
    fs::path root;
    bool found = search_upwards(fs::current_path(ec), root);
    if (!found) {
        const std::string exe_dir = self_exe_dir();
        if (!exe_dir.empty()) found = search_upwards(fs::u8path(exe_dir), root);
    }
    if (!found) {
        err = "config.json not found (searched upwards from the working "
              "directory and from the executable's directory)";
        return false;
    }
    fs::current_path(root, ec);
    if (ec) {
        err = "cannot change directory to " + root.u8string() + ": " + ec.message();
        return false;
    }
    root_out = root.u8string();
    return true;
}

std::string find_peer_exe() {
#ifdef _WIN32
    const char* name = "peer.exe";
#else
    const char* name = "peer";
#endif
    std::vector<fs::path> candidates;
    const std::string dir = self_exe_dir();
    if (!dir.empty()) candidates.push_back(fs::u8path(dir) / name);
    for (const char* sub : {"build/Release", "build/RelWithDebInfo",
                            "build/MinSizeRel", "build/Debug", "build", "."})
        candidates.push_back(fs::path(sub) / name);

    for (const auto& c : candidates) {
        std::error_code ec;
        if (fs::is_regular_file(c, ec)) return fs::absolute(c, ec).u8string();
    }
    return "";
}

}  // namespace proc
