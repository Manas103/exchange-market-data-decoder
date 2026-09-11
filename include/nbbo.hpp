// Cross-venue NBBO aggregation and locked/crossed quote detection.
//
// Regulation NMS conceptually requires a "protected quotation" to be the
// best bid and offer available across every venue trading a symbol, not any
// single venue's own touch (Rule 611), and separately treats a market where
// the best bid at one venue equals or exceeds the best offer at another as
// locked or crossed (Rule 610(e)). Neither rule's real legal text is
// implemented here -- this is the market-structure arithmetic those rules
// are about: given N venues, each with its own best bid and offer, compute
// the National Best Bid and Offer, and flag any pair of distinct venues
// whose quotes lock or cross.
#pragma once
#include <cstdint>
#include <vector>

#include "order_book.hpp"

namespace mdfeed {

struct Quote {
    int venue_id = -1;
    int64_t price_ticks = 0;
    uint32_t qty = 0;
    bool valid = false;
};

enum class QuoteRelation { Locked, Crossed };

// One locked or crossed pair: venue `bid_venue`'s bid versus venue
// `offer_venue`'s offer. Always a distinct pair of venues -- a single
// venue's own book crossing itself is the base repo's documented,
// feed-driven property (see README, Findings: "the book crosses, and that
// is the feed's fault"), not a cross-venue market-structure event, and is
// deliberately not reported by this layer.
struct LockedCrossedPair {
    int bid_venue;
    int offer_venue;
    int64_t bid_ticks;
    int64_t offer_ticks;
    QuoteRelation relation;
};

// Consolidates any number of venue order books into a single NBBO, and
// detects locked/crossed quotes between distinct venues. Stateless: every
// call re-reads the venues' current touch, so the "republished" NBBO is
// always exactly consistent with book state at the moment of the call.
// Ties on price are broken toward the lowest venue index, so results are
// deterministic given the same venue ordering.
class NbboAggregator {
public:
    static Quote best_bid(const std::vector<const OrderBook*>& venues) {
        Quote best;
        for (size_t i = 0; i < venues.size(); ++i) {
            if (!venues[i]->has_bid()) continue;
            const int64_t px = venues[i]->best_bid_ticks();
            if (!best.valid || px > best.price_ticks) {
                best = Quote{static_cast<int>(i), px, static_cast<uint32_t>(venues[i]->best_bid_qty()), true};
            }
        }
        return best;
    }

    static Quote best_offer(const std::vector<const OrderBook*>& venues) {
        Quote best;
        for (size_t i = 0; i < venues.size(); ++i) {
            if (!venues[i]->has_ask()) continue;
            const int64_t px = venues[i]->best_ask_ticks();
            if (!best.valid || px < best.price_ticks) {
                best = Quote{static_cast<int>(i), px, static_cast<uint32_t>(venues[i]->best_ask_qty()), true};
            }
        }
        return best;
    }

    // Every distinct-venue (bid, offer) pair that is locked or crossed.
    // O(N^2) in the number of venues, which is exactly right for N=3; a
    // consolidator quoting hundreds of venues would want a sorted-touch
    // approach instead, out of scope here.
    static std::vector<LockedCrossedPair> classify(const std::vector<const OrderBook*>& venues) {
        std::vector<LockedCrossedPair> out;
        for (size_t i = 0; i < venues.size(); ++i) {
            if (!venues[i]->has_bid()) continue;
            const int64_t bid = venues[i]->best_bid_ticks();
            for (size_t j = 0; j < venues.size(); ++j) {
                if (i == j || !venues[j]->has_ask()) continue;
                const int64_t offer = venues[j]->best_ask_ticks();
                if (bid == offer) {
                    out.push_back(LockedCrossedPair{static_cast<int>(i), static_cast<int>(j), bid, offer,
                                                      QuoteRelation::Locked});
                } else if (bid > offer) {
                    out.push_back(LockedCrossedPair{static_cast<int>(i), static_cast<int>(j), bid, offer,
                                                      QuoteRelation::Crossed});
                }
            }
        }
        return out;
    }
};

// Oracle: recomputes one venue's best bid/offer by scanning every one of its
// 16,384 price levels from scratch, rather than trusting OrderBook's
// incrementally maintained best_bid_idx_/best_ask_idx_. This mirrors the
// base repo's own cross-language oracle (README, Validation #3: "NumPy
// recomputes them from scratch with np.nonzero(...).max()"), in the same
// language this time, over the same public accessors (bid_data/ask_data)
// rather than private state. Deliberately O(kLevels) per call; only used by
// the reference test, never on a hot path.
inline Quote oracle_best_bid(const OrderBook& book, int venue_id) {
    const int64_t* data = book.bid_data();
    for (int idx = OrderBook::kLevels - 1; idx >= 0; --idx) {
        if (data[idx] > 0) {
            return Quote{venue_id, book.base_price_ticks() + idx, static_cast<uint32_t>(data[idx]), true};
        }
    }
    return Quote{};
}

inline Quote oracle_best_ask(const OrderBook& book, int venue_id) {
    const int64_t* data = book.ask_data();
    for (int idx = 0; idx < OrderBook::kLevels; ++idx) {
        if (data[idx] > 0) {
            return Quote{venue_id, book.base_price_ticks() + idx, static_cast<uint32_t>(data[idx]), true};
        }
    }
    return Quote{};
}

}  // namespace mdfeed
