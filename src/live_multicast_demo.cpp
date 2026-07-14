// Real UDP multicast demo: one thread publishes the synthetic feed as
// actual multicast datagrams on loopback (239.255.10.10:30001); the main
// thread joins the group, decodes packets as they arrive, and feeds them
// into a live Normalizer -- this is the part of the project that is
// genuinely "decoding a UDP multicast feed", as opposed to decoder_bench.cpp
// which intentionally bypasses the network to isolate decode/book-update
// cost. UDP has no delivery guarantee, so this program measures and reports
// actual packet loss rather than assuming none occurred.
//
// Messages are batched several-per-datagram (like real exchange multicast
// feeds, which never make one syscall per message) up to ~1200 payload
// bytes -- an early version of this program sent one message per sendto()
// call and was syscall-bound at ~13K msgs/sec on this machine; batching is
// what makes the network path representative of the real thing instead of
// being a Winsock syscall-overhead benchmark.
//
// Usage: live_multicast_demo.exe [num_messages]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include "protocol.hpp"
#include "market_simulator.hpp"
#include "normalizer.hpp"
#include "multicast_udp.hpp"
#include "latency_stats.hpp"

using namespace mdfeed;
using Clock = std::chrono::steady_clock;

namespace {
constexpr const char* kGroup = "239.255.10.10";
constexpr uint16_t kPort = 30001;
constexpr size_t kMaxPayloadBytes = 1200;

std::atomic<bool> g_sender_done{false};
std::atomic<uint64_t> g_sent{0};

#pragma pack(push, 1)
struct PacketHeader {
    uint16_t num_messages;
};
#pragma pack(pop)

} // namespace

void sender_thread(size_t num_messages) {
    UdpMulticastSender sock(kGroup, kPort);
    MarketSimulator sim(/*seed=*/42);

    std::vector<char> packet(kMaxPayloadBytes + sizeof(WireMsg));
    size_t offset = sizeof(PacketHeader);
    uint16_t count = 0;

    auto flush = [&]() {
        if (count == 0) return;
        reinterpret_cast<PacketHeader*>(packet.data())->num_messages = count;
        sock.send(packet.data(), offset);
        offset = sizeof(PacketHeader);
        count = 0;
    };

    for (size_t i = 0; i < num_messages; ++i) {
        WireMsg m = sim.next_message();
        m.header.timestamp_ns = static_cast<uint64_t>(Clock::now().time_since_epoch().count());
        size_t sz = msg_size(m.header.type);
        if (offset + sz > kMaxPayloadBytes) flush();
        std::memcpy(packet.data() + offset, &m, sz);
        offset += sz;
        ++count;
        g_sent.fetch_add(1, std::memory_order_relaxed);
    }
    flush();
    g_sender_done.store(true, std::memory_order_release);
}

int main(int argc, char** argv) {
    size_t num_messages = 2'000'000;
    if (argc > 1) num_messages = std::strtoull(argv[1], nullptr, 10);

    UdpMulticastReceiver receiver(kGroup, kPort);
    receiver.set_recv_timeout_ms(200);

    // Reference prices only -- constructing this instance never calls
    // next_message(), so its per-instrument prices stay at the same
    // initial values the sender's simulator started from (see
    // market_simulator.hpp: prices only drift once next_message() runs).
    MarketSimulator price_ref(/*seed=*/42);
    Normalizer normalizer(/*num_shards=*/4, num_messages / 4 + 1024);
    for (int i = 0; i < price_ref.num_instruments(); ++i) {
        normalizer.init_reference_price(static_cast<uint16_t>(i), price_ref.reference_price_ticks(i));
    }
    normalizer.start();

    std::printf("Joined multicast group %s:%u. Starting publisher for %zu messages...\n",
                kGroup, kPort, num_messages);
    std::thread sender(sender_thread, num_messages);

    std::vector<uint64_t> net_latency_ns;
    net_latency_ns.reserve(num_messages);
    uint64_t received = 0;
    uint64_t packets_received = 0;
    std::vector<char> buf(65536);
    const auto t_start = Clock::now();

    while (true) {
        int n = receiver.recv(buf.data(), buf.size());
        if (n > 0) {
            ++packets_received;
            uint64_t recv_ns = static_cast<uint64_t>(Clock::now().time_since_epoch().count());
            auto* hdr = reinterpret_cast<const PacketHeader*>(buf.data());
            size_t offset = sizeof(PacketHeader);
            for (uint16_t k = 0; k < hdr->num_messages && offset < static_cast<size_t>(n); ++k) {
                WireMsg m{};
                MsgType t = *reinterpret_cast<const MsgType*>(buf.data() + offset);
                size_t sz = msg_size(t);
                std::memcpy(&m, buf.data() + offset, sz);
                offset += sz;
                net_latency_ns.push_back(recv_ns - m.header.timestamp_ns);
                normalizer.dispatch(m);
                ++received;
            }
        } else {
            if (g_sender_done.load(std::memory_order_acquire)) break;
        }
    }
    const auto t_end = Clock::now();
    sender.join();
    normalizer.drain_and_stop();

    const double seconds = std::chrono::duration<double>(t_end - t_start).count();
    std::printf("\n=== Live UDP Multicast Demo (batched) ===\n");
    std::printf("Sent:              %llu messages\n", (unsigned long long)g_sent.load());
    std::printf("Received:          %llu messages in %llu datagrams (avg %.1f msgs/datagram)\n",
                (unsigned long long)received, (unsigned long long)packets_received,
                static_cast<double>(received) / static_cast<double>(packets_received));
    std::printf("Loss:              %.4f%%\n",
                100.0 * (1.0 - static_cast<double>(received) / static_cast<double>(g_sent.load())));
    std::printf("Duration:          %.3f s  (%.0f msgs/sec observed on the wire)\n",
                seconds, static_cast<double>(received) / seconds);
    std::printf("\nSend-to-decode latency (actual OS network stack + decode, in this process):\n");
    auto report = percentiles(net_latency_ns);
    print_report("network+decode", report);
    return 0;
}
