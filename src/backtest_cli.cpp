// CLI entry point for the queue-position backtest simulator.
//
// Deliberately a CLI + CSV file interface rather than a nanobind binding
// like python/mdfeed_ext.cpp. nanobind's value in this repo is a zero-copy
// live view over a book that mutates while Python watches it; the backtest
// is an offline, one-shot batch replay that produces a single flat table of
// finished orders, and Python's only remaining job is arithmetic (fill
// rate, spread P&L, Sharpe) over that table. A CSV artifact is more honest
// about what this is, easier to test end to end without a compiled
// extension, and easier to commit a raw sample of under docs/, so that is
// the interface used here.
//
// Usage:
//   backtest_cli <num_messages> <out_csv_path> [seed] [placement_interval] [ttl_messages] [order_qty]
#include <cstdio>
#include <cstdlib>
#include <string>

#include "backtest_sim.hpp"

using namespace mdfeed;

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr,
                      "usage: %s <num_messages> <out_csv_path> [seed] [placement_interval] "
                      "[ttl_messages] [order_qty]\n",
                      argv[0]);
        return 2;
    }

    BacktestEngine::Config cfg;
    cfg.num_messages = std::strtoull(argv[1], nullptr, 10);
    const std::string out_path = argv[2];
    if (argc > 3) cfg.seed = static_cast<uint32_t>(std::strtoul(argv[3], nullptr, 10));
    if (argc > 4) cfg.placement_interval = std::strtoull(argv[4], nullptr, 10);
    if (argc > 5) cfg.ttl_messages = std::strtoull(argv[5], nullptr, 10);
    if (argc > 6) cfg.order_qty = static_cast<uint32_t>(std::strtoul(argv[6], nullptr, 10));

    std::printf("Queue-position backtest: %llu messages, seed=%u, placement_interval=%llu, "
                "ttl=%llu, order_qty=%u\n",
                (unsigned long long)cfg.num_messages, cfg.seed,
                (unsigned long long)cfg.placement_interval, (unsigned long long)cfg.ttl_messages,
                cfg.order_qty);

    BacktestEngine engine(cfg);
    std::vector<SimOrderResult> results = engine.run();

    FILE* f = std::fopen(out_path.c_str(), "w");
    if (!f) {
        std::fprintf(stderr, "could not open %s for writing\n", out_path.c_str());
        return 1;
    }
    std::fprintf(f,
                  "id,pair_id,model,side,price_ticks,qty,placed_msg_idx,resolved_msg_idx,outcome,"
                  "bid_at_place,ask_at_place,bid_at_fill,ask_at_fill,ahead_qty_at_place\n");
    for (const auto& r : results) {
        std::fprintf(f, "%llu,%llu,%s,%c,%lld,%u,%llu,%llu,%s,%lld,%lld,%lld,%lld,%llu\n",
                      (unsigned long long)r.id, (unsigned long long)r.pair_id,
                      r.is_naive ? "naive" : "queue", r.side == Side::Buy ? 'B' : 'S',
                      (long long)r.price_ticks, r.qty, (unsigned long long)r.placed_msg_idx,
                      (unsigned long long)r.resolved_msg_idx, outcome_name(r.outcome),
                      (long long)r.bid_at_place, (long long)r.ask_at_place,
                      (long long)r.bid_at_fill, (long long)r.ask_at_fill,
                      (unsigned long long)r.ahead_qty_at_place);
    }
    std::fclose(f);

    long long naive_total = 0, naive_filled = 0, naive_canceled = 0, naive_resting = 0;
    long long queue_total = 0, queue_filled = 0, queue_canceled = 0, queue_resting = 0;
    for (const auto& r : results) {
        long long& total = r.is_naive ? naive_total : queue_total;
        ++total;
        long long& filled = r.is_naive ? naive_filled : queue_filled;
        long long& canceled = r.is_naive ? naive_canceled : queue_canceled;
        long long& resting = r.is_naive ? naive_resting : queue_resting;
        switch (r.outcome) {
            case Outcome::Filled: ++filled; break;
            case Outcome::Canceled: ++canceled; break;
            case Outcome::Resting: ++resting; break;
        }
    }

    std::printf("\n%-24s %10s %10s\n", "", "naive", "queue-tracked");
    std::printf("%-24s %10lld %10lld\n", "orders placed", naive_total, queue_total);
    std::printf("%-24s %10lld %10lld\n", "filled", naive_filled, queue_filled);
    std::printf("%-24s %10lld %10lld\n", "canceled (TTL)", naive_canceled, queue_canceled);
    std::printf("%-24s %10lld %10lld\n", "still resting at end", naive_resting, queue_resting);
    std::printf("%-24s %9.2f%% %9.2f%%\n", "fill rate",
                naive_total ? 100.0 * naive_filled / naive_total : 0.0,
                queue_total ? 100.0 * queue_filled / queue_total : 0.0);
    std::printf("\nresults written to %s\n", out_path.c_str());
    return 0;
}
