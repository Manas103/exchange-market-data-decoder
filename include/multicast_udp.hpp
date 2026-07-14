// Minimal Winsock UDP multicast sender/receiver.
//
// Windows-only (this project was built and tested with MSVC/Winsock, not
// cross-platform POSIX sockets) -- a real deployment would abstract this
// behind an interface with a Linux epoll/io_uring backend, but that wasn't
// available to test in this environment, so it's kept explicit rather than
// pretending portability that was never exercised.
#pragma once
#include <winsock2.h>
#include <ws2tcpip.h>
#include <string>
#include <stdexcept>
#pragma comment(lib, "ws2_32.lib")

namespace mdfeed {

inline void wsa_init() {
    static bool done = false;
    if (!done) {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
        done = true;
    }
}

class UdpMulticastSender {
public:
    UdpMulticastSender(const std::string& group_ip, uint16_t port) {
        wsa_init();
        sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (sock_ == INVALID_SOCKET) throw std::runtime_error("socket() failed");
        addr_.sin_family = AF_INET;
        addr_.sin_port = htons(port);
        inet_pton(AF_INET, group_ip.c_str(), &addr_.sin_addr);
        int ttl = 8;
        setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_TTL, reinterpret_cast<const char*>(&ttl), sizeof(ttl));
        u_char loop = 1;
        setsockopt(sock_, IPPROTO_IP, IP_MULTICAST_LOOP, reinterpret_cast<const char*>(&loop), sizeof(loop));
    }
    ~UdpMulticastSender() { if (sock_ != INVALID_SOCKET) closesocket(sock_); }

    int send(const void* data, size_t len) {
        return sendto(sock_, reinterpret_cast<const char*>(data), static_cast<int>(len), 0,
                       reinterpret_cast<sockaddr*>(&addr_), sizeof(addr_));
    }

private:
    SOCKET sock_ = INVALID_SOCKET;
    sockaddr_in addr_{};
};

class UdpMulticastReceiver {
public:
    UdpMulticastReceiver(const std::string& group_ip, uint16_t port) {
        wsa_init();
        sock_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (sock_ == INVALID_SOCKET) throw std::runtime_error("socket() failed");
        int reuse = 1;
        setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_port = htons(port);
        local.sin_addr.s_addr = INADDR_ANY;
        if (bind(sock_, reinterpret_cast<sockaddr*>(&local), sizeof(local)) != 0) {
            throw std::runtime_error("bind() failed");
        }
        ip_mreq mreq{};
        inet_pton(AF_INET, group_ip.c_str(), &mreq.imr_multiaddr);
        mreq.imr_interface.s_addr = INADDR_ANY;
        setsockopt(sock_, IPPROTO_IP, IP_ADD_MEMBERSHIP, reinterpret_cast<const char*>(&mreq), sizeof(mreq));
        // Generous socket receive buffer -- the OS still drops datagrams if
        // the receiver can't drain it fast enough, which is exactly the
        // real-world tradeoff this project measures honestly.
        int rcvbuf = 8 * 1024 * 1024;
        setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&rcvbuf), sizeof(rcvbuf));
    }
    ~UdpMulticastReceiver() { if (sock_ != INVALID_SOCKET) closesocket(sock_); }

    void set_recv_timeout_ms(int ms) {
        setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&ms), sizeof(ms));
    }

    int recv(void* buf, size_t buflen) {
        return recvfrom(sock_, reinterpret_cast<char*>(buf), static_cast<int>(buflen), 0, nullptr, nullptr);
    }

private:
    SOCKET sock_ = INVALID_SOCKET;
};

} // namespace mdfeed
