// Reference-oracle checks for the cross-venue NBBO layer.
//
// Two independent diffs, mirroring the base repo's own "recompute from
// scratch and diff" idiom (see README, Validation #3):
//
// 1. NBBO diff. NbboAggregator::best_bid/best_offer read each OrderBook's
//    incrementally maintained best_bid_idx_/best_ask_idx_ (via
//    best_bid_ticks()/best_ask_ticks()). oracle_best_bid/oracle_best_ask in
//    nbbo.hpp instead scan every one of a book's 16,384 price levels from
//    scratch. Diffed over a long multi-venue replay, agreement is a real
//    check on the incremental bookkeeping, not a tautology, the same way
//    the base repo's np.nonzero(...) oracle is for a single book.
// 2. Locked/crossed diff. NbboAggregator::classify() (reads the same
//    incremental touches as (1)) versus a from-scratch pairwise
//    classification built directly out of the oracle quotes computed in
//    (1) -- two independent code paths over the same book state.
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "nbbo.hpp"
#include "venue_feed.hpp"

using namespace mdfeed;

namespace {

int expect_eq(long long actual, long long expected, const char* what) {
    if (actual != expected) {
        std::printf("FAIL: %s expected=%lld actual=%lld\n", what, expected, actual);
        return 1;
    }
    return 0;
}

int expect_true(bool cond, const char* what) {
    if (!cond) {
        std::printf("FAIL: %s\n", what);
        return 1;
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t messages_per_venue = 90'000;
    uint64_t check_every = 25;
    if (argc > 1) messages_per_venue = std::strtoull(argv[1], nullptr, 10);
    if (argc > 2) check_every = std::strtoull(argv[2], nullptr, 10);

    constexpr int kNumVenues = 3;
    const int64_t ref_price = 25000;  // $250.00, all three venues quote the same symbol
    std::vector<VenueFeed> venues;
    venues.emplace_back(0, /*seed=*/1001, ref_price);
    venues.emplace_back(1, /*seed=*/1002, ref_price);
    venues.emplace_back(2, /*seed=*/1003, ref_price);
    std::vector<const OrderBook*> books = {&venues[0].book(), &venues[1].book(), &venues[2].book()};

    long long nbbo_comparisons = 0, nbbo_mismatches = 0;
    long long lc_comparisons = 0, lc_mismatches = 0;
    long long locked_seen = 0, crossed_seen = 0;

    for (uint64_t i = 0; i < messages_per_venue; ++i) {
        for (int v = 0; v < kNumVenues; ++v) venues[v].step();

        if (i % check_every != 0) continue;

        // ---- 1. NBBO diff ----
        const Quote fast_bid = NbboAggregator::best_bid(books);
        const Quote fast_offer = NbboAggregator::best_offer(books);

        Quote oracle_bid{}, oracle_offer{};
        for (int v = 0; v < kNumVenues; ++v) {
            const Quote ob = oracle_best_bid(*books[v], v);
            if (ob.valid && (!oracle_bid.valid || ob.price_ticks > oracle_bid.price_ticks)) oracle_bid = ob;
            const Quote oo = oracle_best_ask(*books[v], v);
            if (oo.valid && (!oracle_offer.valid || oo.price_ticks < oracle_offer.price_ticks)) oracle_offer = oo;
        }

        ++nbbo_comparisons;
        if (fast_bid.valid != oracle_bid.valid || fast_bid.price_ticks != oracle_bid.price_ticks) {
            std::printf("NBBO BID MISMATCH at msg %llu: fast=%lld(valid=%d) oracle=%lld(valid=%d)\n",
                        (unsigned long long)i, (long long)fast_bid.price_ticks, fast_bid.valid,
                        (long long)oracle_bid.price_ticks, oracle_bid.valid);
            ++nbbo_mismatches;
        }
        ++nbbo_comparisons;
        if (fast_offer.valid != oracle_offer.valid || fast_offer.price_ticks != oracle_offer.price_ticks) {
            std::printf("NBBO OFFER MISMATCH at msg %llu: fast=%lld(valid=%d) oracle=%lld(valid=%d)\n",
                        (unsigned long long)i, (long long)fast_offer.price_ticks, fast_offer.valid,
                        (long long)oracle_offer.price_ticks, oracle_offer.valid);
            ++nbbo_mismatches;
        }

        // ---- 2. Locked/crossed diff ----
        const auto fast_pairs = NbboAggregator::classify(books);

        std::vector<Quote> ob(kNumVenues), oo(kNumVenues);
        for (int v = 0; v < kNumVenues; ++v) {
            ob[v] = oracle_best_bid(*books[v], v);
            oo[v] = oracle_best_ask(*books[v], v);
        }
        std::vector<LockedCrossedPair> oracle_pairs;
        for (int a = 0; a < kNumVenues; ++a) {
            if (!ob[a].valid) continue;
            for (int b = 0; b < kNumVenues; ++b) {
                if (a == b || !oo[b].valid) continue;
                if (ob[a].price_ticks == oo[b].price_ticks) {
                    oracle_pairs.push_back(LockedCrossedPair{a, b, ob[a].price_ticks, oo[b].price_ticks,
                                                               QuoteRelation::Locked});
                    ++locked_seen;
                } else if (ob[a].price_ticks > oo[b].price_ticks) {
                    oracle_pairs.push_back(LockedCrossedPair{a, b, ob[a].price_ticks, oo[b].price_ticks,
                                                               QuoteRelation::Crossed});
                    ++crossed_seen;
                }
            }
        }

        ++lc_comparisons;
        bool mismatch = (fast_pairs.size() != oracle_pairs.size());
        if (!mismatch) {
            for (size_t k = 0; k < fast_pairs.size(); ++k) {
                if (fast_pairs[k].bid_venue != oracle_pairs[k].bid_venue ||
                    fast_pairs[k].offer_venue != oracle_pairs[k].offer_venue ||
                    fast_pairs[k].bid_ticks != oracle_pairs[k].bid_ticks ||
                    fast_pairs[k].offer_ticks != oracle_pairs[k].offer_ticks ||
                    fast_pairs[k].relation != oracle_pairs[k].relation) {
                    mismatch = true;
                    break;
                }
            }
        }
        if (mismatch) {
            std::printf("LOCKED/CROSSED MISMATCH at msg %llu: fast_count=%zu oracle_count=%zu\n",
                        (unsigned long long)i, fast_pairs.size(), oracle_pairs.size());
            ++lc_mismatches;
        }
    }

    std::printf("NBBO diff: %lld comparisons, %lld mismatches.\n", nbbo_comparisons, nbbo_mismatches);
    std::printf(
        "Locked/crossed diff: %lld comparisons, %lld mismatches (locked pairs seen=%lld, crossed pairs "
        "seen=%lld).\n",
        lc_comparisons, lc_mismatches, locked_seen, crossed_seen);
    for (int v = 0; v < kNumVenues; ++v) {
        std::printf("Venue %d dropped-update counter: %llu\n", v, (unsigned long long)venues[v].dropped());
    }

    // ---- 3. Explicit positive locked and crossed cases ----
    // The random replay above almost never produces a naturally locked or
    // crossed market (three independent random walks around the same
    // reference price rarely invert), so `locked pairs seen` and `crossed
    // pairs seen` above are typically 0. That is a real gap: the oracle diff
    // proves classify() agrees with an independent implementation whenever a
    // locked or crossed pair happens to occur, but proves nothing about
    // whether one ever does. These three books are hand-built so a lock and
    // a cross definitely occur, and classify() is checked against the exact
    // expected pair by name.
    long long explicit_failures = 0;
    {
        OrderBook locked_bid_book, locked_offer_book;
        locked_bid_book.init(25000);
        locked_offer_book.init(25000);
        locked_bid_book.add(Side::Buy, 25010, 100);    // venue 0 bids 250.10
        locked_offer_book.add(Side::Sell, 25010, 100);  // venue 1 offers 250.10 -> locked
        std::vector<const OrderBook*> locked_books = {&locked_bid_book, &locked_offer_book};
        auto locked_pairs = NbboAggregator::classify(locked_books);
        bool found_locked = false;
        for (const auto& p : locked_pairs) {
            if (p.bid_venue == 0 && p.offer_venue == 1 && p.relation == QuoteRelation::Locked &&
                p.bid_ticks == 25010 && p.offer_ticks == 25010) {
                found_locked = true;
            }
        }
        if (!found_locked) {
            std::printf("FAIL: explicit locked case not classified as Locked (25010 bid vs 25010 offer)\n");
            ++explicit_failures;
        } else {
            std::printf("Explicit locked case: venue 0 bid 250.10 vs venue 1 offer 250.10 -> classified Locked.\n");
        }
    }
    {
        OrderBook crossed_bid_book, crossed_offer_book;
        crossed_bid_book.init(25000);
        crossed_offer_book.init(25000);
        crossed_bid_book.add(Side::Buy, 25020, 100);    // venue 0 bids 250.20
        crossed_offer_book.add(Side::Sell, 25005, 100);  // venue 1 offers 250.05 -> crossed
        std::vector<const OrderBook*> crossed_books = {&crossed_bid_book, &crossed_offer_book};
        auto crossed_pairs = NbboAggregator::classify(crossed_books);
        bool found_crossed = false;
        for (const auto& p : crossed_pairs) {
            if (p.bid_venue == 0 && p.offer_venue == 1 && p.relation == QuoteRelation::Crossed &&
                p.bid_ticks == 25020 && p.offer_ticks == 25005) {
                found_crossed = true;
            }
        }
        if (!found_crossed) {
            std::printf("FAIL: explicit crossed case not classified as Crossed (25020 bid vs 25005 offer)\n");
            ++explicit_failures;
        } else {
            std::printf("Explicit crossed case: venue 0 bid 250.20 vs venue 1 offer 250.05 -> classified Crossed.\n");
        }
    }

    int failures = 0;
    failures += expect_eq(nbbo_mismatches, 0, "NBBO oracle diff");
    failures += expect_eq(lc_mismatches, 0, "locked/crossed oracle diff");
    failures += expect_true(nbbo_comparisons > 0, "at least one NBBO comparison ran");
    failures += expect_true(lc_comparisons > 0, "at least one locked/crossed comparison ran");
    failures += static_cast<int>(explicit_failures);

    if (failures == 0) {
        std::printf(
            "\nPASS: cross-venue NBBO matches the from-scratch oracle exactly; locked/crossed "
            "classification matches an independently derived pairing exactly.\n");
        return 0;
    }
    std::printf("\n%d CHECK(S) FAILED.\n", failures);
    return 1;
}
