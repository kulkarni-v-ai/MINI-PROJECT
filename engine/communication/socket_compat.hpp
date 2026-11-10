// ============================================================================
// socket_compat.hpp - POSIX / Winsock compatibility layer (E2)
//
// Everything platform specific about sockets lives here, so the protocol,
// client and server code can be written once:
//   * socket_t / INVALID_SOCK / sock_close
//   * one-time Winsock start-up (sock_init is idempotent and thread-safe)
//   * error classification (errno vs WSAGetLastError)
//   * non-blocking switch used for the connect-with-timeout
// ============================================================================
#pragma once

#ifdef _WIN32
    #ifndef NOMINMAX
    #define NOMINMAX
    #endif
    #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #ifdef _MSC_VER
    #pragma comment(lib, "ws2_32.lib")
    #endif

    using socket_t = SOCKET;
    constexpr socket_t INVALID_SOCK = INVALID_SOCKET;

    inline int sock_close(socket_t s) { return ::closesocket(s); }

    // Winsock must be started once per process. WSACleanup is intentionally
    // never called: the OS releases everything at process exit, and calling
    // it while detached worker threads still use sockets would be unsafe.
    inline bool sock_init() {
        static const bool ok = [] {
            WSADATA data;
            return ::WSAStartup(MAKEWORD(2, 2), &data) == 0;
        }();
        return ok;
    }
    inline void sock_cleanup() {}

    inline int  sock_last_error() { return ::WSAGetLastError(); }
    inline bool sock_err_interrupted(int e) { return e == WSAEINTR; }
    inline bool sock_err_timeout(int e) { return e == WSAETIMEDOUT || e == WSAEWOULDBLOCK; }
    inline bool sock_err_in_progress(int e) { return e == WSAEWOULDBLOCK || e == WSAEINPROGRESS; }
    inline bool sock_err_addr_in_use(int e) { return e == WSAEADDRINUSE || e == WSAEACCES; }
    inline bool sock_set_nonblocking(socket_t s, bool on) {
        u_long mode = on ? 1 : 0;
        return ::ioctlsocket(s, FIONBIO, &mode) == 0;
    }
#else
    #include <arpa/inet.h>
    #include <fcntl.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <sys/select.h>
    #include <sys/socket.h>
    #include <unistd.h>

    #include <cerrno>

    using socket_t = int;
    constexpr socket_t INVALID_SOCK = -1;

    #ifndef MSG_NOSIGNAL
    #define MSG_NOSIGNAL 0
    #endif

    inline int  sock_close(socket_t s) { return ::close(s); }
    inline bool sock_init() { return true; }
    inline void sock_cleanup() {}

    inline int  sock_last_error() { return errno; }
    inline bool sock_err_interrupted(int e) { return e == EINTR; }
    inline bool sock_err_timeout(int e) { return e == EAGAIN || e == EWOULDBLOCK || e == ETIMEDOUT; }
    inline bool sock_err_in_progress(int e) { return e == EINPROGRESS || e == EINTR; }
    inline bool sock_err_addr_in_use(int e) { return e == EADDRINUSE; }
    inline bool sock_set_nonblocking(socket_t s, bool on) {
        const int flags = ::fcntl(s, F_GETFL, 0);
        if (flags < 0) return false;
        return ::fcntl(s, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK)) == 0;
    }
#endif
