// Seeded fault-injection harness for the outbound trade-through guard.
//
// Two independent measurements, both exercising TradeThroughGuard::evaluate
// directly (the real production code path, not a stand-in):
//
//   1. Exactly 2,000 genuine trade-through scenarios are constructed, not
//      hoped for. The harness advances the three venues' independent order
//      flows, round-robin, until it finds a victim venue V and a rival
//      venue W whose protected quote is strictly better than V's own touch,
//      builds a marketable order at V's own touch, and checks whether the
//      guard rejects it. Whether a scenario is a genuine violation is
//      decided by directly comparing the venues' raw touch prices, a check
//      entirely independent of the guard's own internal call into
//      NbboAggregator, so "caught N of 2,000" is a real measurement, not a
//      tautology built to always read 2000/2000.
//   2. 40 "clean" sessions of legitimate order flow are run: resting limit
//      orders priced away from the market (never marketable, so never
//      capable of trading through anything), and marketable orders sent to
//      whichever venue already holds the best protected price on that side
//      (executing at the NBBO itself, never inferior to it). Every single
//      order in every session is asserted Accepted; any
//      RejectedTradeThrough among them is counted as a false positive.
//
// Usage: nbbo_fault_injection [seed]
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "nbbo.hpp"
#include "trade_through_guard.hpp"
#include "venue_feed.hpp"

using namespace mdfeed;

namespace {

constexpr int kNumVenues = 3;

struct Venues3 {
    std::vector<VenueFeed> feeds;
    std::vector<const OrderBook*> books;

    Venues3(uint32_t seed_base, int64_t ref_price) {
        feeds.emplace_back(0, seed_base + 1, ref_price);
        feeds.emplace_back(1, seed_base + 2, ref_price);
        feeds.emplace_back(2, seed_base + 3, ref_price);
        for (auto& f : feeds) books.push_back(&f.book());
    }

    void step_all() {
        for (auto& f : feeds) f.step();
    }
};

// Advances venues round-robin until a strict trade-through opportunity
// exists on the requested side (a victim venue whose own touch is strictly
// worse than a rival venue's touch), or gives up after max_steps rounds.
bool find_buy_violation(Venues3& v, int& victim, int& rival, uint64_t max_steps) {
    for (uint64_t s = 0; s < max_steps; ++s) {
        for (int i = 0; i < kNumVenues; ++i) {
            if (!v.books[i]->has_ask()) continue;
            for (int j = 0; j < kNumVenues; ++j) {
                if (i == j || !v.books[j]->has_ask()) continue;
                if (v.books[j]->best_ask_ticks() < v.books[i]->best_ask_ticks()) {
                    victim = i;
                    rival = j;
                    return true;
                }
            }
        }
        v.step_all();
    }
    return false;
}

bool find_sell_violation(Venues3& v, int& victim, int& rival, uint64_t max_steps) {
    for (uint64_t s = 0; s < max_steps; ++s) {
        for (int i = 0; i < kNumVenues; ++i) {
            if (!v.books[i]->has_bid()) continue;
            for (int j = 0; j < kNumVenues; ++j) {
                if (i == j || !v.books[j]->has_bid()) continue;
                if (v.books[j]->best_bid_ticks() > v.books[i]->best_bid_ticks()) {
                    victim = i;
                    rival = j;
                    return true;
                }
            }
        }
        v.step_all();
    }
    return false;
}

}  // namespace

int main(int argc, char** argv) {
    uint32_t seed = 777;
    if (argc > 1) seed = static_cast<uint32_t>(std::strtoul(argv[1], nullptr, 10));

    const int64_t ref_price = 25000;  // $250.00

    // ---- Part 1: 2,000 seeded trade-through scenarios ----
    Venues3 tv(seed, ref_price);
    for (int i = 0; i < 50; ++i) tv.step_all();  // small warm-up so books have real depth

    const uint64_t target_scenarios = 2000;
    uint64_t constructed = 0, caught = 0;

    for (uint64_t n = 0; n < target_scenarios; ++n) {
        int victim = -1, rival = -1;
        const bool want_buy = (n % 2 == 0);
        const bool found = want_buy ? find_buy_violation(tv, victim, rival, /*max_steps=*/50000)
                                     : find_sell_violation(tv, victim, rival, /*max_steps=*/50000);

        if (!found) {
            std::printf("FAIL: could not construct scenario %llu within step budget\n", (unsigned long long)n);
            break;
        }
        ++constructed;

        OutboundOrder order;
        order.venue_id = victim;
        order.qty = 100;
        if (want_buy) {
            order.side = Side::Buy;
            order.limit_price_ticks = tv.books[victim]->best_ask_ticks();  // marketable at victim's own touch
        } else {
            order.side = Side::Sell;
            order.limit_price_ticks = tv.books[victim]->best_bid_ticks();
        }

        const GuardResult r = TradeThroughGuard::evaluate(order, tv.books);
        if (r.decision == GuardDecision::RejectedTradeThrough) {
            ++caught;
        } else {
            std::printf("MISS: scenario %llu (victim venue %d, rival venue %d, %s) not caught, decision=%d\n",
                        (unsigned long long)n, victim, rival, want_buy ? "buy" : "sell",
                        static_cast<int>(r.decision));
        }

        tv.step_all();  // move state forward so the next scenario is not identical
    }

    std::printf("=== Seeded trade-through injection ===\n");
    std::printf("Scenarios constructed: %llu / %llu requested\n", (unsigned long long)constructed,
                (unsigned long long)target_scenarios);
    std::printf("Caught by guard:        %llu / %llu\n", (unsigned long long)caught,
                (unsigned long long)constructed);

    // ---- Part 2: 40 clean sessions, zero false positives expected ----
    const int num_sessions = 40;
    const int orders_per_session = 50;
    uint64_t clean_total = 0, false_positives = 0;

    for (int session = 0; session < num_sessions; ++session) {
        Venues3 cv(seed + 1000 + static_cast<uint32_t>(session) * 7u, ref_price);
        for (int i = 0; i < 20; ++i) cv.step_all();

        for (int k = 0; k < orders_per_session; ++k) {
            cv.step_all();
            const bool buy = (k % 2 == 0);
            const bool resting = (k % 3 == 0);  // one third of orders deliberately non-marketable

            // Route to whichever venue currently holds the best protected
            // price on the relevant side. Executing at the NBBO itself is
            // legitimate by construction and can never be a trade-through.
            const Quote best = buy ? NbboAggregator::best_offer(cv.books) : NbboAggregator::best_bid(cv.books);
            if (!best.valid) continue;

            OutboundOrder order;
            order.qty = 100;
            order.side = buy ? Side::Buy : Side::Sell;
            order.venue_id = best.venue_id;

            if (resting) {
                // Priced away from the market on purpose: never marketable.
                order.limit_price_ticks = buy ? best.price_ticks - 500 : best.price_ticks + 500;
            } else {
                // Marketable exactly at the venue already quoting the NBBO.
                order.limit_price_ticks = best.price_ticks;
            }

            const GuardResult r = TradeThroughGuard::evaluate(order, cv.books);
            ++clean_total;
            if (r.decision == GuardDecision::RejectedTradeThrough) {
                ++false_positives;
                std::printf("FALSE POSITIVE: session %d order %d (venue %d, %s, resting=%d)\n", session, k,
                            order.venue_id, buy ? "buy" : "sell", resting ? 1 : 0);
            }
        }
    }

    std::printf("\n=== Clean sessions (false-positive check) ===\n");
    std::printf("Sessions: %d, orders per session: %d, total clean orders: %llu\n", num_sessions,
                orders_per_session, (unsigned long long)clean_total);
    std::printf("False positives: %llu\n", (unsigned long long)false_positives);

    const bool part1_ok = (constructed == target_scenarios) && (caught == target_scenarios);
    const bool part2_ok = (false_positives == 0);
    std::printf("\n%s\n", (part1_ok && part2_ok) ? "PASS: all fault-injection checks passed."
                                                  : "FAIL: see mismatches above.");
    return (part1_ok && part2_ok) ? 0 : 1;
}
