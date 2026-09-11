// Cross-venue NBBO republish latency benchmark.
//
// Three independent simulated venues each apply their own next order-flow
// message (a "venue book update"). This benchmark times only the republish
// step that follows each update: recomputing the consolidated NBBO and the
// full locked/crossed classification across the three venues. Timing starts
// after VenueFeed::step() returns (the book update itself, which is the
// same decode-and-apply cost already measured in decoder_bench.cpp) and
// stops once the NBBO and locked/crossed set for that update are known --
// which is the definition of "NBBO republished from a venue book update"
// this benchmark uses.
//
// Usage: nbbo_bench [messages_per_venue]
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "latency_stats.hpp"
#include "nbbo.hpp"
#include "venue_feed.hpp"

using namespace mdfeed;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
    uint64_t per_venue = 700'000;
    if (argc > 1) per_venue = std::strtoull(argv[1], nullptr, 10);

    constexpr int kNumVenues = 3;
    const int64_t ref_price = 25000;  // $250.00, all three venues quote the same symbol
    std::vector<VenueFeed> venues;
    venues.emplace_back(0, /*seed=*/5001, ref_price);
    venues.emplace_back(1, /*seed=*/5002, ref_price);
    venues.emplace_back(2, /*seed=*/5003, ref_price);
    std::vector<const OrderBook*> books = {&venues[0].book(), &venues[1].book(), &venues[2].book()};

    std::vector<uint64_t> republish_ns;
    republish_ns.reserve(per_venue * kNumVenues);

    uint64_t locked_events = 0, crossed_events = 0, total_updates = 0;

    std::printf("Running %llu book updates per venue across %d venues (%llu total updates)...\n",
                (unsigned long long)per_venue, kNumVenues, (unsigned long long)(per_venue * kNumVenues));

    for (uint64_t round = 0; round < per_venue; ++round) {
        for (int v = 0; v < kNumVenues; ++v) {
            venues[v].step();  // the venue's own book update; not timed here

            const auto t0 = Clock::now();
            const Quote bid = NbboAggregator::best_bid(books);
            const Quote offer = NbboAggregator::best_offer(books);
            const auto pairs = NbboAggregator::classify(books);
            const auto t1 = Clock::now();

            republish_ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            ++total_updates;
            for (const auto& p : pairs) {
                if (p.relation == QuoteRelation::Locked) ++locked_events;
                else ++crossed_events;
            }
            (void)bid;
            (void)offer;
        }
    }

    std::printf("\n=== Cross-Venue NBBO Republish Benchmark ===\n");
    std::printf("Venues: %d\n", kNumVenues);
    std::printf("Venue book updates: %llu\n", (unsigned long long)total_updates);
    std::printf("\nNBBO republish latency (recompute best bid/offer + locked/crossed classify, per update):\n");
    auto report = percentiles(republish_ns);
    print_report("nbbo_republish", report);
    std::printf("\nLocked-quote pair-events observed:  %llu\n", (unsigned long long)locked_events);
    std::printf("Crossed-quote pair-events observed: %llu\n", (unsigned long long)crossed_events);
    for (int v = 0; v < kNumVenues; ++v) {
        std::printf("Venue %d dropped-update counter: %llu\n", v, (unsigned long long)venues[v].dropped());
    }
    return 0;
}
