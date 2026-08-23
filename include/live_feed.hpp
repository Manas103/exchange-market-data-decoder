// One reusable object for "publish the synthetic feed as real multicast
// datagrams, join the group, decode what arrives, keep 512 full-depth books".
//
// Both entry points share it: src/live_multicast_demo.cpp (the C++ program)
// and python/mdfeed_ext.cpp (the nanobind module). Keeping one implementation
// matters more than it looks, because the Python binding's headline claim is
// that Python is reading the same book memory the decoder writes. If the
// binding had its own copy of the receive loop, "the same memory" would be a
// statement about two similar programs rather than about one program.
//
// Threading: start() spawns a publisher thread and a receiver thread and
// returns immediately, so a caller holding the GIL can drop it and come back.
// wait() joins both and drains every shard queue, after which the books are
// quiescent and safe to read exactly.
#pragma once
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include "protocol.hpp"
#include "market_simulator.hpp"
#include "normalizer.hpp"
#include "multicast_udp.hpp"

namespace mdfeed {

struct FeedStats {
    uint64_t sent = 0;
    uint64_t received = 0;
    uint64_t datagrams = 0;
    double seconds = 0.0;
    double loss_pct = 0.0;
    double msgs_per_sec = 0.0;
    double msgs_per_datagram = 0.0;
};

// Wire framing for the multicast path: several messages per datagram, the
// way real exchange feeds pack them. One message per sendto() is syscall
// bound and measures Winsock, not the decoder.
#pragma pack(push, 1)
struct PacketHeader {
    uint16_t num_messages;
};
#pragma pack(pop)

constexpr size_t kMaxPayloadBytes = 1200;

class LiveFeed {
public:
    LiveFeed(int num_shards, const std::string& group, uint16_t port, size_t expected_msgs)
        : group_(group),
          port_(port),
          expected_msgs_(expected_msgs),
          normalizer_(num_shards, expected_msgs / static_cast<size_t>(num_shards) + 1024),
          price_ref_(/*seed=*/42) {
        // price_ref_ never calls next_message(), so its per-instrument
        // reference prices stay at exactly the values the publisher's
        // simulator starts from (prices only drift once next_message runs).
        for (int i = 0; i < price_ref_.num_instruments(); ++i) {
            normalizer_.init_reference_price(static_cast<uint16_t>(i),
                                             price_ref_.reference_price_ticks(i));
        }
        receiver_ = std::make_unique<UdpMulticastReceiver>(group_, port_);
        receiver_->set_recv_timeout_ms(200);
        normalizer_.start();
        latency_ns_.reserve(expected_msgs_);
    }

    // The normalizer's shard threads are started in the constructor, so they
    // must be joined even if the caller never started a run. Destroying a
    // joinable std::thread calls std::terminate, which is exactly what an
    // early version of this class did the first time a test built a feed and
    // dropped it without publishing anything.
    ~LiveFeed() {
        try {
            if (running_) wait();
            if (!drained_) {
                normalizer_.drain_and_stop();
                drained_ = true;
            }
        } catch (...) {
        }
    }

    LiveFeed(const LiveFeed&) = delete;
    LiveFeed& operator=(const LiveFeed&) = delete;

    void start(size_t num_messages) {
        if (running_) throw std::runtime_error("feed already running");
        if (num_messages > expected_msgs_) {
            throw std::runtime_error("num_messages exceeds the capacity this feed was sized for");
        }
        running_ = true;
        num_messages_ = num_messages;
        t_start_ = std::chrono::steady_clock::now();
        publisher_ = std::thread([this] { publish_loop(); });
        consumer_ = std::thread([this] { receive_loop(); });
    }

    // Blocks until the publisher is finished, the receiver has timed out on a
    // quiet socket, and every shard queue has drained.
    void wait() {
        if (!running_) return;
        if (publisher_.joinable()) publisher_.join();
        if (consumer_.joinable()) consumer_.join();
        if (!drained_) {
            normalizer_.drain_and_stop();
            drained_ = true;
        }
        running_ = false;
        stats_.sent = sent_.load();
        stats_.received = received_;
        stats_.datagrams = datagrams_;
        stats_.seconds = std::chrono::duration<double>(t_end_ - t_start_).count();
        stats_.loss_pct = stats_.sent == 0 ? 0.0
                          : 100.0 * (1.0 - static_cast<double>(stats_.received) /
                                               static_cast<double>(stats_.sent));
        stats_.msgs_per_sec = stats_.seconds > 0.0
                                  ? static_cast<double>(stats_.received) / stats_.seconds
                                  : 0.0;
        stats_.msgs_per_datagram = stats_.datagrams == 0
                                       ? 0.0
                                       : static_cast<double>(stats_.received) /
                                             static_cast<double>(stats_.datagrams);
    }

    bool running() const { return running_; }
    const FeedStats& stats() const { return stats_; }
    uint64_t received_so_far() const { return received_snapshot_.load(std::memory_order_relaxed); }
    const std::vector<uint64_t>& latencies() const { return latency_ns_; }
    // Non-const handle for percentiles(), which sorts in place.
    std::vector<uint64_t>& latencies_mut() { return latency_ns_; }
    int num_instruments() const { return price_ref_.num_instruments(); }
    int64_t reference_price_ticks(int i) const { return price_ref_.reference_price_ticks(i); }
    const OrderBook& book(uint16_t instrument) const {
        return normalizer_.book_for_instrument(instrument);
    }
    const Normalizer& normalizer() const { return normalizer_; }

private:
    void publish_loop() {
        UdpMulticastSender sock(group_, port_);
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

        for (size_t i = 0; i < num_messages_; ++i) {
            WireMsg m = sim.next_message();
            m.header.timestamp_ns =
                static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            size_t sz = msg_size(m.header.type);
            if (offset + sz > kMaxPayloadBytes) flush();
            std::memcpy(packet.data() + offset, &m, sz);
            offset += sz;
            ++count;
            sent_.fetch_add(1, std::memory_order_relaxed);
        }
        flush();
        publisher_done_.store(true, std::memory_order_release);
    }

    void receive_loop() {
        std::vector<char> buf(65536);
        while (true) {
            int n = receiver_->recv(buf.data(), buf.size());
            if (n > 0) {
                ++datagrams_;
                uint64_t recv_ns =
                    static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
                auto* hdr = reinterpret_cast<const PacketHeader*>(buf.data());
                size_t offset = sizeof(PacketHeader);
                for (uint16_t k = 0; k < hdr->num_messages && offset < static_cast<size_t>(n); ++k) {
                    WireMsg m{};
                    MsgType t = *reinterpret_cast<const MsgType*>(buf.data() + offset);
                    size_t sz = msg_size(t);
                    std::memcpy(&m, buf.data() + offset, sz);
                    offset += sz;
                    latency_ns_.push_back(recv_ns - m.header.timestamp_ns);
                    normalizer_.dispatch(m);
                    ++received_;
                }
                received_snapshot_.store(received_, std::memory_order_relaxed);
            } else {
                if (publisher_done_.load(std::memory_order_acquire)) break;
            }
        }
        t_end_ = std::chrono::steady_clock::now();
    }

    std::string group_;
    uint16_t port_;
    size_t expected_msgs_;
    size_t num_messages_ = 0;
    Normalizer normalizer_;
    MarketSimulator price_ref_;
    std::unique_ptr<UdpMulticastReceiver> receiver_;
    std::thread publisher_;
    std::thread consumer_;
    std::atomic<bool> publisher_done_{false};
    std::atomic<uint64_t> sent_{0};
    std::atomic<uint64_t> received_snapshot_{0};
    uint64_t received_ = 0;
    uint64_t datagrams_ = 0;
    std::vector<uint64_t> latency_ns_;
    std::chrono::steady_clock::time_point t_start_{};
    std::chrono::steady_clock::time_point t_end_{};
    bool running_ = false;
    bool drained_ = false;
    FeedStats stats_{};
};

// Offline replay of the same message stream, no sockets involved. The Python
// tests use two of these (one shard versus eight) as a reference build to diff
// against, which is the cross-language version of tests/correctness_test.cpp:
// the diff runs in NumPy over zero-copy views of both books rather than in C++.
class OfflineBooks {
public:
    OfflineBooks(size_t num_messages, int num_shards, uint32_t seed)
        : normalizer_(num_shards, num_messages / static_cast<size_t>(num_shards) + 1024),
          sim_(seed) {
        std::vector<WireMsg> messages(num_messages);
        for (size_t i = 0; i < num_messages; ++i) messages[i] = sim_.next_message();
        MarketSimulator prices(seed);
        for (int i = 0; i < prices.num_instruments(); ++i) {
            normalizer_.init_reference_price(static_cast<uint16_t>(i),
                                             prices.reference_price_ticks(i));
            ref_price_.push_back(prices.reference_price_ticks(i));
        }
        normalizer_.start();
        for (const auto& m : messages) {
            while (!normalizer_.dispatch(m)) {}
        }
        normalizer_.drain_and_stop();
    }

    int num_instruments() const { return static_cast<int>(ref_price_.size()); }
    int64_t reference_price_ticks(int i) const { return ref_price_[i]; }
    const OrderBook& book(uint16_t instrument) const {
        return normalizer_.book_for_instrument(instrument);
    }

private:
    Normalizer normalizer_;
    MarketSimulator sim_;
    std::vector<int64_t> ref_price_;
};

} // namespace mdfeed
