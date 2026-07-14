// Offline throughput/latency benchmark for the decoder + normalizer.
//
// Messages are pre-generated into memory before timing starts so the
// benchmark measures decode + order-book-maintenance cost only -- not
// random-number generation, and not the network stack (see
// live_multicast_demo.cpp for a real UDP multicast run, which is bounded
// by socket throughput rather than CPU and is reported separately).
//
// Usage: decoder_bench.exe [num_messages] [num_shards]
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <chrono>
#include "protocol.hpp"
#include "market_simulator.hpp"
#include "normalizer.hpp"
#include "latency_stats.hpp"

using namespace mdfeed;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
    size_t num_messages = 20'000'000;
    int num_shards = 8;
    if (argc > 1) num_messages = std::strtoull(argv[1], nullptr, 10);
    if (argc > 2) num_shards = std::atoi(argv[2]);

    std::printf("Generating %zu messages across %d instruments...\n", num_messages, NUM_INSTRUMENTS);
    MarketSimulator sim(/*seed=*/42);
    std::vector<WireMsg> messages(num_messages);
    for (size_t i = 0; i < num_messages; ++i) {
        messages[i] = sim.next_message();
    }

    Normalizer normalizer(num_shards, num_messages / num_shards + 1024);
    for (int i = 0; i < sim.num_instruments(); ++i) {
        normalizer.init_reference_price(static_cast<uint16_t>(i), sim.reference_price_ticks(i));
    }

    std::printf("Starting %d normalizer worker threads...\n", num_shards);
    normalizer.start();

    std::printf("Dispatching...\n");
    const auto t_start = Clock::now();
    for (const auto& m : messages) {
        while (!normalizer.dispatch(m)) {
            // Backpressure: a shard queue is full. In production this would
            // be a signal to add shards or shed load; here we just spin
            // until the worker drains a slot so no message is lost.
        }
    }
    normalizer.drain_and_stop();
    const auto t_end = Clock::now();

    const double seconds = std::chrono::duration<double>(t_end - t_start).count();
    const double msgs_per_sec = static_cast<double>(num_messages) / seconds;

    std::vector<uint64_t> all_proc, all_e2e;
    uint64_t total_processed = 0, total_dropped_full = 0;
    for (int i = 0; i < normalizer.num_shards(); ++i) {
        Shard& s = normalizer.shard(i);
        total_processed += s.processed.load();
        total_dropped_full += s.dropped_full.load();
        all_proc.insert(all_proc.end(), s.proc_latency_ns.begin(), s.proc_latency_ns.end());
        all_e2e.insert(all_e2e.end(), s.e2e_latency_ns.begin(), s.e2e_latency_ns.end());
    }

    std::printf("\n=== Decoder + Normalizer Benchmark ===\n");
    std::printf("Messages dispatched:        %zu\n", num_messages);
    std::printf("Messages processed:         %llu (dropped_full=%llu)\n",
                 (unsigned long long)total_processed, (unsigned long long)total_dropped_full);
    std::printf("Wall time:                  %.3f s\n", seconds);
    std::printf("Throughput:                 %.0f msgs/sec  (%d shards)\n", msgs_per_sec, num_shards);
    std::printf("\nPer-message processing latency (decode + book update, single message):\n");
    auto proc_report = percentiles(all_proc);
    print_report("proc_latency", proc_report);
    std::printf("\nEnd-to-end latency (dispatch enqueue -> book updated):\n");
    auto e2e_report = percentiles(all_e2e);
    print_report("e2e_latency", e2e_report);

    std::printf("\n(Instrument order books use a bounds-checked fixed-size price-level\n"
                " array; see order_book.hpp for the dropped-update counter if a price\n"
                " ever moved outside the pre-sized band.)\n");
    return 0;
}
