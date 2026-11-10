// ============================================================================
// process_manager_windows.cpp - native Win32 process control
//
//   * peers are launched with CreateProcessW (no cmd.exe, no PowerShell, no
//     taskkill) with a correctly quoted command line, an absolute executable
//     path, an explicit working directory and UTF-16 paths throughout;
//   * the child's stdin is the NUL device and stdout/stderr go to
//     run\peer_<id>.log; PROC_THREAD_ATTRIBUTE_HANDLE_LIST restricts handle
//     inheritance to exactly those handles, so nothing else of the controller
//     (e.g. a redirected stdout pipe) leaks into the long-lived peers;
//   * every HANDLE is owned by an RAII wrapper;
//   * a pid is only trusted if the process image really is peer.exe, so a
//     recycled pid can never make `fail`/`sys stop` hit an unrelated process;
//   * graceful stop = per-process named event; TerminateProcess is the
//     fallback and is also what `fail` uses to simulate a crash;
//   * no administrator rights needed (only handles to our own child processes
//     are opened).
// ============================================================================
#include "process_manager.hpp"

#ifndef _WIN32
#error process_manager_windows.cpp is Windows-only
#endif

#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <shellapi.h>

#include <atomic>
#include <cwctype>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

namespace proc {
namespace {

// ---- UTF-8 <-> UTF-16 ----------------------------------------------------------
std::wstring widen(const std::string& s) {
    if (s.empty()) return std::wstring();
    const int n = ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), nullptr, 0);
    if (n <= 0) return std::wstring();
    std::wstring w(static_cast<size_t>(n), L'\0');
    ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), &w[0], n);
    return w;
}

std::string narrow(const std::wstring& w) {
    if (w.empty()) return std::string();
    const int n = ::WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()),
                                        nullptr, 0, nullptr, nullptr);
    if (n <= 0) return std::string();
    std::string s(static_cast<size_t>(n), '\0');
    ::WideCharToMultiByte(CP_UTF8, 0, w.data(), static_cast<int>(w.size()),
                          &s[0], n, nullptr, nullptr);
    return s;
}

std::string win_error(const char* what, DWORD code) {
    return std::string(what) + " (Win32 error " + std::to_string(code) + ")";
}

// ---- RAII HANDLE -----------------------------------------------------------------
class Handle {
public:
    Handle() = default;
    explicit Handle(HANDLE h) : h_(h) {}
    ~Handle() { reset(); }
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& o) noexcept : h_(o.h_) { o.h_ = nullptr; }
    Handle& operator=(Handle&& o) noexcept {
        if (this != &o) { reset(); h_ = o.h_; o.h_ = nullptr; }
        return *this;
    }
    bool valid() const { return h_ != nullptr && h_ != INVALID_HANDLE_VALUE; }
    explicit operator bool() const { return valid(); }
    HANDLE get() const { return h_; }
    void reset() {
        if (valid()) ::CloseHandle(h_);
        h_ = nullptr;
    }
private:
    HANDLE h_ = nullptr;
};

// ---- command-line quoting ------------------------------------------------------
// Quotes one argument so that CommandLineToArgvW / the MSVC CRT parse it back
// to exactly `arg` (backslashes before a quote are doubled, embedded quotes
// escaped, trailing backslashes doubled).
void append_quoted(std::wstring& cmd, const std::wstring& arg) {
    if (!arg.empty() && arg.find_first_of(L" \t\n\v\"") == std::wstring::npos) {
        cmd += arg;
        return;
    }
    cmd.push_back(L'"');
    for (auto it = arg.begin();; ++it) {
        size_t backslashes = 0;
        while (it != arg.end() && *it == L'\\') { ++it; ++backslashes; }
        if (it == arg.end()) {
            cmd.append(backslashes * 2, L'\\');
            break;
        }
        if (*it == L'"') {
            cmd.append(backslashes * 2 + 1, L'\\');
            cmd.push_back(L'"');
        } else {
            cmd.append(backslashes, L'\\');
            cmd.push_back(*it);
        }
    }
    cmd.push_back(L'"');
}

// ---- process identity --------------------------------------------------------------
bool image_is_peer(HANDLE process) {
    wchar_t buf[32768];
    DWORD len = static_cast<DWORD>(sizeof(buf) / sizeof(buf[0]));
    if (!::QueryFullProcessImageNameW(process, 0, buf, &len)) return false;
    std::wstring path(buf, len);
    const size_t slash = path.find_last_of(L"\\/");
    std::wstring name = (slash == std::wstring::npos) ? path : path.substr(slash + 1);
    for (auto& ch : name) ch = static_cast<wchar_t>(std::towlower(ch));
    return name == L"peer.exe";
}

std::wstring stop_event_name(DWORD pid) {
    return L"Local\\ResilientFileEngine.Peer." + std::to_wstring(pid) + L".Stop";
}

// ---- peer-side shutdown watcher ------------------------------------------------------
HANDLE g_stop_event = nullptr;
std::thread g_watcher;
std::atomic<bool> g_quit{false};
std::function<void()> g_callback;

BOOL WINAPI console_handler(DWORD type) {
    switch (type) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            if (g_stop_event) ::SetEvent(g_stop_event);
            return TRUE;
        default:
            return FALSE;
    }
}

}  // namespace

// ============================================================================
bool start_peer(const std::string& exe_path, const std::string& id,
                const std::string& address, uint16_t port, long& pid_out,
                std::string& err, const std::string& coord_address,
                uint16_t coord_port) {
    std::error_code ec;
    fs::create_directories("run", ec);

    const std::wstring exe = widen(exe_path);
    if (exe.empty() || ::GetFileAttributesW(exe.c_str()) == INVALID_FILE_ATTRIBUTES) {
        err = "peer executable not found: " + exe_path;
        return false;
    }

    // Working directory = current directory (the project root).
    std::wstring cwd(MAX_PATH, L'\0');
    DWORD cwd_len = ::GetCurrentDirectoryW(static_cast<DWORD>(cwd.size()), &cwd[0]);
    if (cwd_len >= cwd.size()) {
        cwd.resize(cwd_len + 1);
        cwd_len = ::GetCurrentDirectoryW(static_cast<DWORD>(cwd.size()), &cwd[0]);
    }
    if (cwd_len == 0) {
        err = win_error("GetCurrentDirectoryW failed", ::GetLastError());
        return false;
    }
    cwd.resize(cwd_len);

    // Inheritable handles for the child: NUL as stdin, the log as stdout/stderr.
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;

    Handle nul(::CreateFileW(L"NUL", GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE,
                             &sa, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr));
    if (!nul) {
        err = win_error("cannot open NUL", ::GetLastError());
        return false;
    }
    const std::wstring log_path = widen(log_file_for(id));
    Handle log(::CreateFileW(log_path.c_str(), GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE, &sa, CREATE_ALWAYS,
                             FILE_ATTRIBUTE_NORMAL, nullptr));
    if (!log) {
        err = win_error(("cannot open log file " + log_file_for(id)).c_str(), ::GetLastError());
        return false;
    }

    // Only these two handles may be inherited (duplicates are not allowed in
    // the list, and stdout/stderr share one handle).
    HANDLE inherit[2] = {nul.get(), log.get()};
    SIZE_T attr_size = 0;
    ::InitializeProcThreadAttributeList(nullptr, 1, 0, &attr_size);
    std::vector<char> attr_buf(attr_size);
    auto* attrs = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(attr_buf.data());
    if (!::InitializeProcThreadAttributeList(attrs, 1, 0, &attr_size)) {
        err = win_error("InitializeProcThreadAttributeList failed", ::GetLastError());
        return false;
    }
    struct AttrGuard {
        LPPROC_THREAD_ATTRIBUTE_LIST a;
        ~AttrGuard() { ::DeleteProcThreadAttributeList(a); }
    } attr_guard{attrs};
    if (!::UpdateProcThreadAttribute(attrs, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                                     inherit, sizeof(inherit), nullptr, nullptr)) {
        err = win_error("UpdateProcThreadAttribute failed", ::GetLastError());
        return false;
    }

    STARTUPINFOEXW si{};
    si.StartupInfo.cb = sizeof(si);
    si.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    si.StartupInfo.hStdInput = nul.get();
    si.StartupInfo.hStdOutput = log.get();
    si.StartupInfo.hStdError = log.get();
    si.lpAttributeList = attrs;

    // "<exe>" <id> <address> <port>, each argument quoted per the CRT rules.
    // CreateProcessW may modify the command-line buffer, so it gets its own copy.
    std::wstring cmd;
    append_quoted(cmd, exe);
    for (const std::string& a : {id, address, std::to_string(port)}) {
        cmd.push_back(L' ');
        append_quoted(cmd, widen(a));
    }
    if (!coord_address.empty() && coord_port != 0) {
        cmd.push_back(L' ');
        append_quoted(cmd, widen(coord_address));
        cmd.push_back(L' ');
        append_quoted(cmd, widen(std::to_string(coord_port)));
    }
    std::vector<wchar_t> cmd_buf(cmd.begin(), cmd.end());
    cmd_buf.push_back(L'\0');

    // Prefer to break away from a parent job object (so peers outlive a
    // controller started inside a kill-on-close job); if the job forbids
    // that, retry without.
    const DWORD base_flags = EXTENDED_STARTUPINFO_PRESENT | CREATE_NO_WINDOW |
                             CREATE_NEW_PROCESS_GROUP;
    PROCESS_INFORMATION pi{};
    BOOL ok = ::CreateProcessW(exe.c_str(), cmd_buf.data(), nullptr, nullptr, TRUE,
                               base_flags | CREATE_BREAKAWAY_FROM_JOB, nullptr,
                               cwd.c_str(), &si.StartupInfo, &pi);
    if (!ok && ::GetLastError() == ERROR_ACCESS_DENIED) {
        ok = ::CreateProcessW(exe.c_str(), cmd_buf.data(), nullptr, nullptr, TRUE,
                              base_flags, nullptr, cwd.c_str(), &si.StartupInfo, &pi);
    }
    if (!ok) {
        err = win_error("CreateProcessW failed", ::GetLastError());
        return false;
    }
    Handle process(pi.hProcess);
    Handle thread(pi.hThread);

    pid_out = static_cast<long>(pi.dwProcessId);
    if (!write_pid_file(pid_file_for(id), pid_out)) {
        err = "cannot write " + pid_file_for(id);
        return false;
    }
    return true;  // all handles closed by their owners
}

bool peer_running(long pid) {
    if (pid <= 0) return false;
    Handle h(::OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, FALSE,
                           static_cast<DWORD>(pid)));
    if (!h) return false;
    if (::WaitForSingleObject(h.get(), 0) != WAIT_TIMEOUT) return false;  // exited
    return image_is_peer(h.get());
}

bool terminate_peer(long pid, std::string& err) {
    if (pid <= 0) return true;
    Handle h(::OpenProcess(PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
                           FALSE, static_cast<DWORD>(pid)));
    if (!h) {
        const DWORD e = ::GetLastError();
        if (e == ERROR_INVALID_PARAMETER) return true;  // no such process
        err = win_error("OpenProcess failed", e);
        return false;
    }
    if (::WaitForSingleObject(h.get(), 0) == WAIT_OBJECT_0) return true;  // already gone
    if (!image_is_peer(h.get())) {
        err = "pid " + std::to_string(pid) + " is not a peer process (stale pid file?)";
        return false;
    }
    if (!::TerminateProcess(h.get(), 1)) {
        if (::WaitForSingleObject(h.get(), 0) == WAIT_OBJECT_0) return true;
        err = win_error("TerminateProcess failed", ::GetLastError());
        return false;
    }
    if (::WaitForSingleObject(h.get(), 5000) != WAIT_OBJECT_0) {
        err = "process did not exit after TerminateProcess";
        return false;
    }
    return true;
}

bool stop_peer(long pid, std::string& err) {
    if (pid <= 0) return true;
    Handle h(::OpenProcess(PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
                           FALSE, static_cast<DWORD>(pid)));
    if (!h) {
        const DWORD e = ::GetLastError();
        if (e == ERROR_INVALID_PARAMETER) return true;
        err = win_error("OpenProcess failed", e);
        return false;
    }
    if (::WaitForSingleObject(h.get(), 0) == WAIT_OBJECT_0) return true;
    if (!image_is_peer(h.get())) {
        err = "pid " + std::to_string(pid) + " is not a peer process (stale pid file?)";
        return false;
    }
    // Polite request first: signal the peer's named stop event.
    Handle ev(::OpenEventW(EVENT_MODIFY_STATE, FALSE, stop_event_name(static_cast<DWORD>(pid)).c_str()));
    if (ev) {
        ::SetEvent(ev.get());
        if (::WaitForSingleObject(h.get(), 3000) == WAIT_OBJECT_0) return true;
    }
    // Not answered in time (or no event): escalate.
    if (!::TerminateProcess(h.get(), 1)) {
        if (::WaitForSingleObject(h.get(), 0) == WAIT_OBJECT_0) return true;
        err = win_error("TerminateProcess failed", ::GetLastError());
        return false;
    }
    if (::WaitForSingleObject(h.get(), 5000) != WAIT_OBJECT_0) {
        err = "process did not exit after TerminateProcess";
        return false;
    }
    return true;
}

std::string self_exe_dir() {
    std::wstring buf(MAX_PATH, L'\0');
    for (;;) {
        const DWORD n = ::GetModuleFileNameW(nullptr, &buf[0], static_cast<DWORD>(buf.size()));
        if (n == 0) return "";
        if (n < buf.size()) { buf.resize(n); break; }
        buf.resize(buf.size() * 2);
    }
    const size_t slash = buf.find_last_of(L"\\/");
    if (slash == std::wstring::npos) return "";
    return narrow(buf.substr(0, slash));
}

std::vector<std::string> command_line_args(int argc, char** argv) {
    std::vector<std::string> out;
    int wargc = 0;
    LPWSTR* wargv = ::CommandLineToArgvW(::GetCommandLineW(), &wargc);
    if (wargv && wargc > 0) {
        for (int i = 0; i < wargc; ++i) out.push_back(narrow(wargv[i]));
        ::LocalFree(wargv);
        return out;
    }
    if (wargv) ::LocalFree(wargv);
    return std::vector<std::string>(argv, argv + argc);
}

void install_shutdown_handler(std::function<void()> on_shutdown) {
    g_callback = std::move(on_shutdown);
    g_quit = false;
    // Manual-reset event named after our pid so the controller can find it.
    g_stop_event = ::CreateEventW(nullptr, TRUE, FALSE,
                                  stop_event_name(::GetCurrentProcessId()).c_str());
    if (!g_stop_event) return;  // no graceful path; controller will terminate
    ::SetConsoleCtrlHandler(console_handler, TRUE);
    g_watcher = std::thread([] {
        ::WaitForSingleObject(g_stop_event, INFINITE);
        if (!g_quit && g_callback) g_callback();
    });
}

void remove_shutdown_handler() {
    g_quit = true;
    if (g_stop_event) ::SetEvent(g_stop_event);  // release the watcher
    if (g_watcher.joinable()) g_watcher.join();
    ::SetConsoleCtrlHandler(console_handler, FALSE);
    if (g_stop_event) {
        ::CloseHandle(g_stop_event);
        g_stop_event = nullptr;
    }
}

}  // namespace proc
