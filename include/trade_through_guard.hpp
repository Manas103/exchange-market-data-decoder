// Outbound order guard implementing the trade-through-avoidance logic behind
// Reg NMS Rule 611 in software: reject an order that would execute at a
// price inferior to a better protected quotation available at another venue
// at the moment the order is sent. This is not the real Rule 611 -- there is
// no Intermarket Sweep Order exception, no self-help exception, no
// flickering-quote exception, no size/depth-of-book nuance -- it is the core
// arithmetic the rule is about: don't let an order execute through a better
// price you could see.
//
// What "would execute" means here: an order is treated as marketable, and
// therefore capable of trading through, only if its limit price crosses its
// OWN venue's current touch (the price it would actually receive there). A
// resting limit order that is not marketable at its own venue cannot trade
// through anything at send time, because it does not execute at send time;
// it is accepted and left resting. This mirrors how Rule 611 itself only
// applies to trades, not to the mere existence of a locked or crossed
// market (see nbbo.hpp for that, separate, detector).
#pragma once
#include <cstdint>
#include <optional>
#include <vector>

#include "nbbo.hpp"
#include "order_book.hpp"

namespace mdfeed {

struct OutboundOrder {
    Side side;
    int64_t limit_price_ticks;
    uint32_t qty;
    int venue_id;  // which venue this order is being sent to
};

enum class GuardDecision { Accepted, RejectedTradeThrough, RejectedNoLiquidity };

struct GuardResult {
    GuardDecision decision;
    std::optional<Quote> better_quote;  // the protected quote that would have been traded through
    int64_t own_touch_ticks = 0;
};

class TradeThroughGuard {
public:
    // `venues` must be indexable by venue_id (venues[order.venue_id] is the
    // book the order is being sent to); the other entries are read only to
    // find a better protected price.
    static GuardResult evaluate(const OutboundOrder& order, const std::vector<const OrderBook*>& venues) {
        const OrderBook& own = *venues[order.venue_id];

        if (order.side == Side::Buy) {
            if (!own.has_ask()) return GuardResult{GuardDecision::RejectedNoLiquidity, std::nullopt, 0};
            const int64_t own_touch = own.best_ask_ticks();
            if (order.limit_price_ticks < own_touch) {
                // Not marketable at its own venue: it rests. Nothing
                // executes at send time, so there is nothing to trade
                // through yet.
                return GuardResult{GuardDecision::Accepted, std::nullopt, own_touch};
            }
            const Quote nbo = NbboAggregator::best_offer(venues);
            if (nbo.valid && nbo.venue_id != order.venue_id && nbo.price_ticks < own_touch) {
                return GuardResult{GuardDecision::RejectedTradeThrough, nbo, own_touch};
            }
            return GuardResult{GuardDecision::Accepted, std::nullopt, own_touch};
        } else {
            if (!own.has_bid()) return GuardResult{GuardDecision::RejectedNoLiquidity, std::nullopt, 0};
            const int64_t own_touch = own.best_bid_ticks();
            if (order.limit_price_ticks > own_touch) {
                return GuardResult{GuardDecision::Accepted, std::nullopt, own_touch};
            }
            const Quote nbb = NbboAggregator::best_bid(venues);
            if (nbb.valid && nbb.venue_id != order.venue_id && nbb.price_ticks > own_touch) {
                return GuardResult{GuardDecision::RejectedTradeThrough, nbb, own_touch};
            }
            return GuardResult{GuardDecision::Accepted, std::nullopt, own_touch};
        }
    }
};

}  // namespace mdfeed
