// ============================================================================
// process_manager.hpp - platform abstraction for peer process control
//
// Deliberately isolated from E1-E6. Two implementations:
//   process_manager_windows.cpp  - Win32 (CreateProcessW, no cmd/PowerShell)
//   process_manager_linux.cpp    - POSIX (fork/exec, signals)
// plus process_manager_common.cpp (pid files, project-root discovery) which
// is shared and contains no platform calls.
//
// All strings crossing this interface are UTF-8.
// ============================================================================
#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace proc {

// ---- run/ bookkeeping (relative to the project root = working directory) ----
std::string pid_file_for(const std::string& peer_id);   // run/peer_<id>.pid
std::string log_file_for(const std::string& peer_id);   // run/peer_<id>.log
long read_pid_file(const std::string& path);             // 0 if missing/invalid
bool write_pid_file(const std::string& path, long pid);
void remove_pid_file(const std::string& path);

// ---- locating things ---------------------------------------------------------
// Directory containing the running executable.
std::string self_exe_dir();
// Finds the project root (the directory holding config.json) by walking up
// from the working directory and then from the executable's directory, and
// makes it the working directory. All relative paths (config.json, run/,
// metadata/, storage/, ...) are therefore independent of where the user
// launched the program from.
bool enter_project_root(std::string& root_out, std::string& err);
// Absolute path of the peer executable, next to the running controller
// (falls back to common build-tree layouts). Empty if not found.
std::string find_peer_exe();
// Command-line arguments as UTF-8 (on Windows taken from the wide command
// line, so non-ASCII paths survive; on POSIX identical to argv).
std::vector<std::string> command_line_args(int argc, char** argv);

// ---- controller side ------------------------------------------------------------
// Starts one peer as a detached background process: working directory = the
// project root, stdin = null device, stdout+stderr = run/peer_<id>.log. No
// console window, no handle leaks into the child. Writes the pid file.
bool start_peer(const std::string& exe_path, const std::string& peer_id,
                const std::string& address, uint16_t port, long& pid_out,
                std::string& err, const std::string& coord_address = "",
                uint16_t coord_port = 0);
// True only if `pid` is alive AND is a peer process (guards against pid reuse).
bool peer_running(long pid);
// Immediate, unclean termination (used to simulate a failure).
bool terminate_peer(long pid, std::string& err);
// Graceful shutdown request, escalating to termination if it is ignored.
bool stop_peer(long pid, std::string& err);

// ---- peer side --------------------------------------------------------------------
// Arranges for `on_shutdown` to run (on a helper thread, never in a signal
// context) when the controller asks the peer to stop: SIGINT/SIGTERM on POSIX,
// a per-process named event / console control events on Windows.
void install_shutdown_handler(std::function<void()> on_shutdown);
// Joins the helper thread; call after the server loop has returned.
void remove_shutdown_handler();

}  // namespace proc
