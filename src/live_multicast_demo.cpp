// Real UDP multicast demo: a publisher thread sends the synthetic feed as
// actual multicast datagrams on loopback (239.255.10.10:30001); a receiver
// thread joins the group, decodes packets as they arrive, and feeds them into
// a live Normalizer. This is the part of the project that is genuinely
// decoding a UDP multicast feed, as opposed to decoder_bench.cpp which
// intentionally bypasses the network to isolate decode and book-update cost.
// UDP has no delivery guarantee, so this program measures and reports actual
// packet loss rather than assuming none occurred.
//
// Messages are batched several per datagram (like real exchange multicast
// feeds, which never make one syscall per message) up to about 1200 payload
// bytes. An early version of this program sent one message per sendto() call
// and was syscall bound at roughly 13K msgs/sec on this machine; batching is
// what makes the network path representative of the real thing instead of
// being a Winsock syscall-overhead benchmark.
//
// The publish/receive/normalize machinery lives in include/live_feed.hpp so
// that the Python binding drives the identical code path.
//
// Usage: live_multicast_demo.exe [num_messages]
#include <cstdio>
#include <cstdlib>
#include "live_feed.hpp"
#include "latency_stats.hpp"

using namespace mdfeed;

int main(int argc, char** argv) {
    size_t num_messages = 2'000'000;
    if (argc > 1) num_messages = std::strtoull(argv[1], nullptr, 10);

    LiveFeed feed(/*num_shards=*/4, "239.255.10.10", 30001, num_messages + 1024);
    std::printf("Joined multicast group 239.255.10.10:30001. Starting publisher for %zu messages...\n",
                num_messages);
    feed.start(num_messages);
    feed.wait();

    const FeedStats& s = feed.stats();
    std::printf("\n=== Live UDP Multicast Demo (batched) ===\n");
    std::printf("Sent:              %llu messages\n", (unsigned long long)s.sent);
    std::printf("Received:          %llu messages in %llu datagrams (avg %.1f msgs/datagram)\n",
                (unsigned long long)s.received, (unsigned long long)s.datagrams,
                s.msgs_per_datagram);
    std::printf("Loss:              %.4f%%\n", s.loss_pct);
    std::printf("Duration:          %.3f s  (%.0f msgs/sec observed on the wire)\n",
                s.seconds, s.msgs_per_sec);
    std::printf("\nSend-to-decode latency (actual OS network stack + decode, in this process):\n");
    auto report = percentiles(feed.latencies_mut());
    print_report("network+decode", report);
    return 0;
}
