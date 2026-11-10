// ============================================================================
// process_manager_linux.cpp - POSIX process control
// ============================================================================
#include "process_manager.hpp"
#ifdef _WIN32
#error process_manager_linux.cpp is POSIX-only
#endif

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

namespace fs = std::filesystem;

namespace proc {

namespace {

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

// Peer processes are started as "<dir>/peer"; make sure a recycled pid does
// not point at some unrelated program. Where /proc is unavailable (macOS)
// the check is skipped.
bool cmdline_is_peer(long pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/cmdline", std::ios::binary);
    if (!in) return fs::exists("/proc/self") ? false : true;
    std::string argv0;
    std::getline(in, argv0, '\0');
    return fs::path(argv0).filename() == "peer";
}

bool is_zombie(long pid) {
    std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
    if (!in) return false;
    std::string line;
    std::getline(in, line);
    const auto rp = line.rfind(')');
    return rp != std::string::npos && rp + 2 < line.size() &&
           (line[rp + 2] == 'Z' || line[rp + 2] == 'X');
}

bool wait_until_gone(long pid, int timeout_ms) {
    for (int waited = 0; waited <= timeout_ms; waited += 20) {
        if (!peer_running(pid)) return true;
        sleep_ms(20);
    }
    return !peer_running(pid);
}

std::atomic<bool> g_quit{false};
volatile std::sig_atomic_t g_signalled = 0;
std::thread g_watcher;
std::function<void()> g_callback;

extern "C" void on_signal(int) { g_signalled = 1; }  // async-signal-safe

}  // namespace

bool start_peer(const std::string& exe_path, const std::string& id,
                const std::string& address, uint16_t port, long& pid_out,
                std::string& err, const std::string& /*coord_address*/,
                uint16_t /*coord_port*/) {
    std::error_code ec;
    fs::create_directories("run", ec);
    if (!fs::is_regular_file(exe_path, ec)) {
        err = "peer executable not found: " + exe_path;
        return false;
    }
    const std::string log = log_file_for(id);
    const std::string port_s = std::to_string(port);

    int errpipe[2];
    if (::pipe(errpipe) != 0) { err = "pipe() failed"; return false; }
    ::fcntl(errpipe[1], F_SETFD, FD_CLOEXEC);  // closed by a successful exec

    const pid_t pid = ::fork();
    if (pid < 0) {
        ::close(errpipe[0]);
        ::close(errpipe[1]);
        err = "fork failed";
        return false;
    }
    if (pid == 0) {
        ::setsid();  // detach from the controller's terminal / process group
        const int in = ::open("/dev/null", O_RDONLY);
        if (in >= 0) { ::dup2(in, STDIN_FILENO); if (in > STDERR_FILENO) ::close(in); }
        const int fd = ::open(log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            ::dup2(fd, STDOUT_FILENO);
            ::dup2(fd, STDERR_FILENO);
            if (fd > STDERR_FILENO) ::close(fd);
        }
        // Do not leak the controller's other descriptors (e.g. a captured
        // stdout pipe) into the long-lived peer.
        long maxfd = ::sysconf(_SC_OPEN_MAX);
        if (maxfd < 0 || maxfd > 4096) maxfd = 4096;
        for (int f = 3; f < maxfd; ++f)
            if (f != errpipe[1]) ::close(f);
        ::execl(exe_path.c_str(), exe_path.c_str(), id.c_str(), address.c_str(),
                port_s.c_str(), static_cast<char*>(nullptr));
        const int e = errno;
        ssize_t ignored = ::write(errpipe[1], &e, sizeof(e));
        (void)ignored;
        ::_exit(127);
    }

    ::close(errpipe[1]);
    int child_errno = 0;
    ssize_t n;
    do { n = ::read(errpipe[0], &child_errno, sizeof(child_errno)); }
    while (n < 0 && errno == EINTR);
    ::close(errpipe[0]);
    if (n == static_cast<ssize_t>(sizeof(child_errno))) {
        ::waitpid(pid, nullptr, 0);
        err = std::string("exec failed: ") + std::strerror(child_errno);
        return false;
    }

    pid_out = static_cast<long>(pid);
    if (!write_pid_file(pid_file_for(id), pid_out)) {
        err = "cannot write " + pid_file_for(id);
        return false;
    }
    return true;
}

bool peer_running(long pid) {
    if (pid <= 0) return false;
    if (::kill(static_cast<pid_t>(pid), 0) != 0 && errno != EPERM) return false;
    int status = 0;  // reap it if it is our own exited child
    if (::waitpid(static_cast<pid_t>(pid), &status, WNOHANG) == static_cast<pid_t>(pid))
        return false;
    if (is_zombie(pid)) return false;
    return cmdline_is_peer(pid);
}

bool terminate_peer(long pid, std::string& err) {
    if (!peer_running(pid)) return true;
    if (::kill(static_cast<pid_t>(pid), SIGKILL) != 0 && errno != ESRCH) {
        err = std::string("kill(SIGKILL) failed: ") + std::strerror(errno);
        return false;
    }
    if (!wait_until_gone(pid, 5000)) {
        err = "process did not exit after SIGKILL";
        return false;
    }
    return true;
}

bool stop_peer(long pid, std::string& err) {
    if (!peer_running(pid)) return true;
    if (::kill(static_cast<pid_t>(pid), SIGTERM) != 0 && errno != ESRCH) {
        err = std::string("kill(SIGTERM) failed: ") + std::strerror(errno);
        return false;
    }
    if (wait_until_gone(pid, 3000)) return true;
    return terminate_peer(pid, err);  // ignored the request: escalate
}

std::string self_exe_dir() {
    std::error_code ec;
    fs::path p = fs::read_symlink("/proc/self/exe", ec);
    if (ec || p.empty()) return "";
    return p.parent_path().u8string();
}

std::vector<std::string> command_line_args(int argc, char** argv) {
    return std::vector<std::string>(argv, argv + argc);
}

void install_shutdown_handler(std::function<void()> on_shutdown) {
    g_callback = std::move(on_shutdown);
    g_quit = false;
    g_signalled = 0;
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
    g_watcher = std::thread([] {
        while (!g_quit) {
            if (g_signalled) {
                if (g_callback) g_callback();
                return;
            }
            sleep_ms(25);
        }
    });
}

void remove_shutdown_handler() {
    g_quit = true;
    if (g_watcher.joinable()) g_watcher.join();
    ::signal(SIGINT, SIG_DFL);
    ::signal(SIGTERM, SIG_DFL);
}

}  // namespace proc
