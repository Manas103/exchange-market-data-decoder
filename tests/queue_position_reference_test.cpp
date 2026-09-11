// Reference-oracle check for queue-position aging.
//
// Two independent checks:
//
// 1. A hand-computed scenario (a handful of Add/Cancel/Execute messages)
//    where every intermediate ahead-quantity value and the exact fill
//    message are worked out by hand and asserted exactly. This is the
//    small, human-checkable case the playbook asks for.
// 2. A long replay through the real synthetic message generator, where
//    dozens of hypothetical resting orders are spawned at different points
//    in the stream and FastAheadTracker's O(1) incremental ahead-quantity
//    is diffed, after every message, against
//    QueuePositionBook::qty_ahead_of_seq, which recomputes the same
//    quantity from scratch by scanning the level's live FIFO list. The two
//    are genuinely different code paths (incremental vs from-scratch), so
//    agreement across tens of thousands of messages and dozens of
//    concurrently tracked positions is real evidence, not a tautology.
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <vector>

#include "market_simulator.hpp"
#include "order_book.hpp"
#include "protocol.hpp"
#include "queue_book.hpp"

using namespace mdfeed;

namespace {

WireMsg make_add(uint16_t instr, uint64_t order_id, Side side, int64_t price, uint32_t qty) {
    WireMsg m{};
    m.add.header.type = MsgType::AddOrder;
    m.add.header.instrument_id = instr;
    m.add.header.timestamp_ns = 0;
    m.add.order_id = order_id;
    m.add.side = side;
    m.add.price_ticks = price;
    m.add.qty = qty;
    return m;
}

WireMsg make_exec(uint16_t instr, uint64_t order_id, uint32_t exec_qty) {
    WireMsg m{};
    m.exec.header.type = MsgType::Execute;
    m.exec.header.instrument_id = instr;
    m.exec.header.timestamp_ns = 0;
    m.exec.order_id = order_id;
    m.exec.exec_qty = exec_qty;
    return m;
}

WireMsg make_cancel(uint16_t instr, uint64_t order_id, uint32_t cancel_qty) {
    WireMsg m{};
    m.cancel.header.type = MsgType::Cancel;
    m.cancel.header.instrument_id = instr;
    m.cancel.header.timestamp_ns = 0;
    m.cancel.order_id = order_id;
    m.cancel.cancel_qty = cancel_qty;
    return m;
}

int expect_eq(long long actual, long long expected, const char* what, int step) {
    if (actual != expected) {
        std::printf("FAIL step %d: %s expected=%lld actual=%lld\n", step, what, expected, actual);
        return 1;
    }
    return 0;
}

// Check 1: hand-computed scenario.
int hand_computed_scenario() {
    int failures = 0;
    QueuePositionBook qbook;
    const uint16_t instr = 0;
    const int64_t price = 10000;

    qbook.apply(make_add(instr, 1, Side::Buy, price, 50));  // seq 0
    qbook.apply(make_add(instr, 2, Side::Buy, price, 30));  // seq 1

    // Snapshot our hypothetical order here: seq 2, ahead = 50 + 30 = 80.
    LevelKey key{instr, Side::Buy, price};
    uint64_t our_seq = qbook.current_seq();
    failures += expect_eq(our_seq, 2, "our_seq at snapshot", 0);
    uint64_t ahead0 = qbook.level_total_qty(key);
    failures += expect_eq(ahead0, 80, "ahead qty at snapshot", 0);
    FastAheadTracker tracker(key, our_seq, ahead0);
    failures += expect_eq(tracker.ahead_qty(), 80, "tracker ahead after construction", 0);

    // Order 3 arrives after our snapshot (seq 2, tied with our_seq): behind us.
    ApplyEffect e1 = qbook.apply(make_add(instr, 3, Side::Buy, price, 20));  // seq 2
    bool fill1 = tracker.consume(e1);
    failures += expect_eq(fill1, false, "fill after order3 add (behind, no reduction)", 1);
    failures += expect_eq(tracker.ahead_qty(), 80, "ahead after order3 add (behind us)", 1);

    // Cancel order 1 (ahead of us, full cancel of 50): must advance us.
    ApplyEffect e2 = qbook.apply(make_cancel(instr, 1, 50));
    bool fill2 = tracker.consume(e2);
    failures += expect_eq(fill2, false, "fill on cancel (never fills)", 2);
    failures += expect_eq(tracker.ahead_qty(), 30, "ahead after cancel of order1 (ahead)", 2);

    // Partial execute of order 2 (ahead of us, 10 of 30): advances us, no fill yet.
    ApplyEffect e3 = qbook.apply(make_exec(instr, 2, 10));
    bool fill3 = tracker.consume(e3);
    failures += expect_eq(fill3, false, "no fill: still not at front", 3);
    failures += expect_eq(tracker.ahead_qty(), 20, "ahead after partial execute of order2", 3);

    // Cancel order 3 (behind us): must NOT advance us.
    ApplyEffect e4 = qbook.apply(make_cancel(instr, 3, 20));
    bool fill4 = tracker.consume(e4);
    failures += expect_eq(fill4, false, "no fill on cancel behind us", 4);
    failures += expect_eq(tracker.ahead_qty(), 20, "ahead unchanged: order3 cancel is behind us", 4);

    // Execute the remaining 20 of order 2 (ahead of us): reaches the front,
    // but the event that reaches the front does not itself fill us.
    ApplyEffect e5 = qbook.apply(make_exec(instr, 2, 20));
    bool fill5 = tracker.consume(e5);
    failures += expect_eq(fill5, false, "reaching the front is not itself a fill", 5);
    failures += expect_eq(tracker.ahead_qty(), 0, "ahead is zero: we are at the front", 5);
    failures += expect_eq(tracker.at_front(), true, "at_front true", 5);

    // A new order arrives behind us, then trades: since we are at the
    // front, this next real trade at this price level fills us.
    qbook.apply(make_add(instr, 4, Side::Buy, price, 15));  // seq 3, behind us
    ApplyEffect e6 = qbook.apply(make_exec(instr, 4, 5));
    bool fill6 = tracker.consume(e6);
    failures += expect_eq(fill6, true, "fill: at front, real trade occurs", 6);

    if (failures == 0) std::printf("PASS: hand-computed scenario, 6 steps, all exact.\n");
    return failures;
}

// Check 2: long replay, fast tracker diffed against the from-scratch oracle.
int long_replay_diff(uint64_t num_messages) {
    struct Tracked {
        LevelKey level;
        uint64_t seq;
        uint64_t spawned_at;
        FastAheadTracker tracker;
    };

    MarketSimulator sim(/*seed=*/321, /*num_instruments=*/1);
    QueuePositionBook qbook;
    OrderBook book;
    book.init(sim.reference_price_ticks(0));
    struct Loc { Side side; int64_t price; uint32_t qty; };
    std::unordered_map<uint64_t, Loc> book_orders;

    std::deque<Tracked> tracked;
    const uint64_t spawn_every = 400;
    const uint64_t max_age = 2500;
    const size_t max_concurrent = 10;

    long long mismatches = 0;
    long long comparisons = 0;
    long long fills_observed = 0;

    for (uint64_t i = 0; i < num_messages; ++i) {
        WireMsg m = sim.next_message();

        switch (m.header.type) {
            case MsgType::AddOrder:
                book_orders[m.add.order_id] = Loc{m.add.side, m.add.price_ticks, m.add.qty};
                book.add(m.add.side, m.add.price_ticks, m.add.qty);
                break;
            case MsgType::Execute: {
                auto it = book_orders.find(m.exec.order_id);
                if (it != book_orders.end()) {
                    uint32_t rq = std::min(m.exec.exec_qty, it->second.qty);
                    book.reduce(it->second.side, it->second.price, rq);
                    it->second.qty -= rq;
                    if (it->second.qty == 0) book_orders.erase(it);
                }
                break;
            }
            case MsgType::Cancel: {
                auto it = book_orders.find(m.cancel.order_id);
                if (it != book_orders.end()) {
                    uint32_t rq = std::min(m.cancel.cancel_qty, it->second.qty);
                    book.reduce(it->second.side, it->second.price, rq);
                    it->second.qty -= rq;
                    if (it->second.qty == 0) book_orders.erase(it);
                }
                break;
            }
            case MsgType::Delete: {
                auto it = book_orders.find(m.del.order_id);
                if (it != book_orders.end()) {
                    book.reduce(it->second.side, it->second.price, it->second.qty);
                    book_orders.erase(it);
                }
                break;
            }
            case MsgType::Replace: {
                auto it = book_orders.find(m.replace.old_order_id);
                if (it != book_orders.end()) {
                    Side side = it->second.side;
                    book.reduce(side, it->second.price, it->second.qty);
                    book_orders.erase(it);
                    book_orders[m.replace.new_order_id] = Loc{side, m.replace.price_ticks, m.replace.qty};
                    book.add(side, m.replace.price_ticks, m.replace.qty);
                }
                break;
            }
        }

        ApplyEffect eff = qbook.apply(m);

        for (auto& t : tracked) {
            if (t.tracker.consume(eff)) ++fills_observed;
        }

        for (const auto& t : tracked) {
            uint64_t oracle = qbook.qty_ahead_of_seq(t.level, t.seq);
            ++comparisons;
            if (static_cast<int64_t>(oracle) != t.tracker.ahead_qty()) {
                std::printf("MISMATCH at message %llu: oracle=%llu fast=%lld (spawned at %llu)\n",
                            (unsigned long long)i, (unsigned long long)oracle,
                            (long long)t.tracker.ahead_qty(), (unsigned long long)t.spawned_at);
                ++mismatches;
            }
        }

        while (!tracked.empty() && i - tracked.front().spawned_at > max_age) tracked.pop_front();

        if (i % spawn_every == 0 && book.has_bid() && tracked.size() < max_concurrent) {
            LevelKey key{0, Side::Buy, book.best_bid_ticks()};
            uint64_t seq = qbook.current_seq();
            uint64_t ahead = qbook.level_total_qty(key);
            tracked.push_back(Tracked{key, seq, i, FastAheadTracker(key, seq, ahead)});
        }
    }

    std::printf("Long replay: %llu messages, %lld ahead-qty comparisons, %lld fills observed, %lld mismatches.\n",
                (unsigned long long)num_messages, comparisons, fills_observed, mismatches);
    if (mismatches == 0) {
        std::printf("PASS: fast incremental tracker matches the from-scratch oracle exactly.\n");
    }
    return static_cast<int>(mismatches > 0 ? 1 : 0);
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t num_messages = 80'000;
    if (argc > 1) num_messages = std::strtoull(argv[1], nullptr, 10);

    int failures = 0;
    failures += hand_computed_scenario();
    failures += long_replay_diff(num_messages);

    if (failures == 0) {
        std::printf("\nALL QUEUE-POSITION REFERENCE CHECKS PASSED.\n");
        return 0;
    }
    std::printf("\n%d CHECK GROUP(S) FAILED.\n", failures);
    return 1;
}
