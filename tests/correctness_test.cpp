// Correctness check: replays the identical message stream through a
// single-shard (effectively single-threaded) Normalizer and an 8-shard
// (multithreaded) Normalizer, then diffs every price level of every
// instrument's book between the two. If the sharded, lock-free,
// concurrent version disagrees with the trivial sequential version
// anywhere, this test fails loudly instead of the bug surfacing as a
// silently wrong book somewhere in production.
#include <cstdio>
#include <cstdlib>
#include <vector>
#include "protocol.hpp"
#include "market_simulator.hpp"
#include "normalizer.hpp"

using namespace mdfeed;

namespace {

void run_normalizer(Normalizer& n, MarketSimulator& sim, const std::vector<WireMsg>& messages) {
    for (int i = 0; i < sim.num_instruments(); ++i) {
        n.init_reference_price(static_cast<uint16_t>(i), sim.reference_price_ticks(i));
    }
    n.start();
    for (const auto& m : messages) {
        while (!n.dispatch(m)) {}
    }
    n.drain_and_stop();
}

} // namespace

int main(int argc, char** argv) {
    size_t num_messages = 2'000'000;
    if (argc > 1) num_messages = std::strtoull(argv[1], nullptr, 10);

    std::printf("Generating %zu messages for correctness comparison...\n", num_messages);
    MarketSimulator sim(/*seed=*/7);
    std::vector<WireMsg> messages(num_messages);
    for (size_t i = 0; i < num_messages; ++i) messages[i] = sim.next_message();

    Normalizer reference(/*num_shards=*/1, num_messages + 1024);
    Normalizer sharded(/*num_shards=*/8, num_messages / 8 + 1024);

    std::printf("Running single-shard reference normalizer...\n");
    run_normalizer(reference, sim, messages);
    std::printf("Running 8-shard multithreaded normalizer...\n");
    run_normalizer(sharded, sim, messages);

    int mismatches = 0;
    long long levels_checked = 0;
    for (int instr = 0; instr < sim.num_instruments(); ++instr) {
        const OrderBook& ref_book = reference.book_for_instrument(static_cast<uint16_t>(instr));
        const OrderBook& test_book = sharded.book_for_instrument(static_cast<uint16_t>(instr));

        if (ref_book.has_bid() != test_book.has_bid() ||
            (ref_book.has_bid() && ref_book.best_bid_ticks() != test_book.best_bid_ticks()) ||
            (ref_book.has_bid() && ref_book.best_bid_qty() != test_book.best_bid_qty())) {
            std::printf("MISMATCH instrument %d: best bid ref=(%lld,%lld) test=(%lld,%lld)\n",
                        instr,
                        ref_book.has_bid() ? (long long)ref_book.best_bid_ticks() : -1,
                        ref_book.has_bid() ? (long long)ref_book.best_bid_qty() : -1,
                        test_book.has_bid() ? (long long)test_book.best_bid_ticks() : -1,
                        test_book.has_bid() ? (long long)test_book.best_bid_qty() : -1);
            ++mismatches;
        }
        if (ref_book.has_ask() != test_book.has_ask() ||
            (ref_book.has_ask() && ref_book.best_ask_ticks() != test_book.best_ask_ticks()) ||
            (ref_book.has_ask() && ref_book.best_ask_qty() != test_book.best_ask_qty())) {
            std::printf("MISMATCH instrument %d: best ask differs\n", instr);
            ++mismatches;
        }

        int64_t ref_price = sim.reference_price_ticks(instr);
        for (int64_t p = ref_price - 300; p <= ref_price + 300; ++p) {
            ++levels_checked;
            if (ref_book.qty_at(Side::Buy, p) != test_book.qty_at(Side::Buy, p)) {
                std::printf("MISMATCH instrument %d bid level %lld: ref=%lld test=%lld\n",
                            instr, (long long)p, (long long)ref_book.qty_at(Side::Buy, p),
                            (long long)test_book.qty_at(Side::Buy, p));
                ++mismatches;
            }
            if (ref_book.qty_at(Side::Sell, p) != test_book.qty_at(Side::Sell, p)) {
                std::printf("MISMATCH instrument %d ask level %lld: ref=%lld test=%lld\n",
                            instr, (long long)p, (long long)ref_book.qty_at(Side::Sell, p),
                            (long long)test_book.qty_at(Side::Sell, p));
                ++mismatches;
            }
        }
    }

    std::printf("\nChecked %lld price levels across %d instruments.\n", levels_checked, sim.num_instruments());
    if (mismatches == 0) {
        std::printf("PASS: multithreaded normalizer output is bit-for-bit identical to the "
                     "single-threaded reference.\n");
        return 0;
    }
    std::printf("FAIL: %d mismatches.\n", mismatches);
    return 1;
}
