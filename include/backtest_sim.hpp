// Queue-position backtest simulator.
//
// Replays the decoder's own synthetic message stream (market_simulator.hpp,
// protocol.hpp) through a single-threaded copy of the same OrderBook class
// the live decoder uses, plus the new QueuePositionBook FIFO layer
// (queue_book.hpp), and simulates resting limit orders under two fill
// models evaluated side by side on identical placement decisions:
//
//   - naive: assumes a resting order at the touch fills the instant the
//     touch trades at all, ignoring queue position entirely.
//   - queue-tracked: an order only fills once every order ahead of it has
//     been canceled or executed away (FastAheadTracker reaches the front),
//     and a real trade then occurs.
//
// Everything about the order book and the message stream is reused; the
// only new state machine here is the queue-position aging and the two fill
// models built on top of it. See README, "Extended" section, for the full
// design writeup and measured results.
#pragma once
#include <algorithm>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

#include "market_simulator.hpp"
#include "order_book.hpp"
#include "protocol.hpp"
#include "queue_book.hpp"

namespace mdfeed {

enum class Outcome { Filled, Canceled, Resting };

inline const char* outcome_name(Outcome o) {
    switch (o) {
        case Outcome::Filled: return "FILLED";
        case Outcome::Canceled: return "CANCELED";
        case Outcome::Resting: return "RESTING";
    }
    return "?";
}

struct SimOrderResult {
    uint64_t id = 0;
    uint64_t pair_id = 0;   // naive and queue-tracked orders placed at the same
                            // instant, same price, same side share a pair_id
    bool is_naive = false;
    Side side = Side::Buy;
    int64_t price_ticks = 0;
    uint32_t qty = 0;
    uint64_t placed_msg_idx = 0;
    uint64_t resolved_msg_idx = 0;
    Outcome outcome = Outcome::Resting;
    int64_t bid_at_place = 0, ask_at_place = 0;
    int64_t bid_at_fill = 0, ask_at_fill = 0;
    uint64_t ahead_qty_at_place = 0;  // 0 for the naive model, by construction of the claim
};

// A resting order our own order book (order_id -> location) does NOT know
// about, because it never appears on the wire; this mirrors what Normalizer
// keeps privately, deshared for a single-instrument, single-threaded,
// deterministic replay loop. A backtest replay does not need the live
// decoder's sharding or threading, which exist to hide network and syscall
// latency during live ingestion; duplicating this ~20-line switch is more
// honest than repurposing Normalizer's threaded worker for something it was
// not built for.
struct RestingLoc {
    Side side;
    int64_t price_ticks;
    uint32_t qty;
};

inline void apply_to_book(const WireMsg& m, OrderBook& book, std::unordered_map<uint64_t, RestingLoc>& orders) {
    switch (m.header.type) {
        case MsgType::AddOrder: {
            const auto& a = m.add;
            orders[a.order_id] = RestingLoc{a.side, a.price_ticks, a.qty};
            book.add(a.side, a.price_ticks, a.qty);
            break;
        }
        case MsgType::Execute: {
            const auto& e = m.exec;
            auto it = orders.find(e.order_id);
            if (it == orders.end()) break;
            uint32_t rq = std::min(e.exec_qty, it->second.qty);
            book.reduce(it->second.side, it->second.price_ticks, rq);
            it->second.qty -= rq;
            if (it->second.qty == 0) orders.erase(it);
            break;
        }
        case MsgType::Cancel: {
            const auto& c = m.cancel;
            auto it = orders.find(c.order_id);
            if (it == orders.end()) break;
            uint32_t rq = std::min(c.cancel_qty, it->second.qty);
            book.reduce(it->second.side, it->second.price_ticks, rq);
            it->second.qty -= rq;
            if (it->second.qty == 0) orders.erase(it);
            break;
        }
        case MsgType::Delete: {
            const auto& d = m.del;
            auto it = orders.find(d.order_id);
            if (it == orders.end()) break;
            book.reduce(it->second.side, it->second.price_ticks, it->second.qty);
            orders.erase(it);
            break;
        }
        case MsgType::Replace: {
            const auto& r = m.replace;
            auto it = orders.find(r.old_order_id);
            if (it == orders.end()) break;
            Side side = it->second.side;
            book.reduce(side, it->second.price_ticks, it->second.qty);
            orders.erase(it);
            orders[r.new_order_id] = RestingLoc{side, r.price_ticks, r.qty};
            book.add(side, r.price_ticks, r.qty);
            break;
        }
    }
}

class BacktestEngine {
public:
    struct Config {
        uint32_t seed = 42;
        uint64_t num_messages = 2'000'000;
        uint64_t placement_interval = 500;  // attempt a new pair of orders every N messages
        uint64_t ttl_messages = 5000;       // cancel if unresolved after this many messages
        uint32_t order_qty = 100;
    };

    explicit BacktestEngine(Config cfg) : cfg_(cfg) {}

    std::vector<SimOrderResult> run() {
        MarketSimulator sim(cfg_.seed, /*num_instruments=*/1);
        book_.init(sim.reference_price_ticks(0));

        std::vector<SimOrderResult> results;
        results.reserve(cfg_.num_messages / cfg_.placement_interval * 2 + 16);

        for (uint64_t msg_idx = 0; msg_idx < cfg_.num_messages; ++msg_idx) {
            WireMsg m = sim.next_message();
            apply_to_book(m, book_, book_orders_);
            ApplyEffect eff = qbook_.apply(m);

            for (auto it = active_.begin(); it != active_.end();) {
                bool resolved = false;
                if (it->is_naive) {
                    if (eff.reduction && eff.reduction->level == it->level && eff.reduction->is_execute) {
                        finish(results, *it, Outcome::Filled, msg_idx);
                        resolved = true;
                    }
                } else if (it->tracker->consume(eff)) {
                    finish(results, *it, Outcome::Filled, msg_idx);
                    resolved = true;
                }
                if (!resolved && msg_idx - it->placed_msg_idx >= cfg_.ttl_messages) {
                    finish(results, *it, Outcome::Canceled, msg_idx);
                    resolved = true;
                }
                it = resolved ? active_.erase(it) : std::next(it);
            }

            if (msg_idx % cfg_.placement_interval == 0 && book_.has_bid() && book_.has_ask()) {
                place_pair(msg_idx);
            }
        }

        for (auto& ao : active_) finish(results, ao, Outcome::Resting, cfg_.num_messages);
        active_.clear();

        return results;
    }

    // Exposed for cross-checks between the two book representations that
    // are fed the identical message stream (see tests/backtest_invariants_test.cpp).
    const OrderBook& book() const { return book_; }
    const QueuePositionBook& qbook() const { return qbook_; }

private:
    struct ActiveOrder {
        uint64_t id;
        uint64_t pair_id;
        bool is_naive;
        LevelKey level;
        uint64_t placed_msg_idx;
        int64_t bid_at_place, ask_at_place;
        uint32_t qty;
        uint64_t ahead_qty_at_place;
        std::optional<FastAheadTracker> tracker;
    };

    void place_pair(uint64_t msg_idx) {
        int64_t px = book_.best_bid_ticks();
        LevelKey key{0, Side::Buy, px};
        uint64_t our_seq = qbook_.current_seq();
        uint64_t ahead = qbook_.level_total_qty(key);
        int64_t bid = book_.best_bid_ticks();
        int64_t ask = book_.best_ask_ticks();
        uint64_t pair_id = next_pair_id_++;

        active_.push_back(ActiveOrder{next_id_++, pair_id, /*is_naive=*/true, key, msg_idx, bid, ask,
                                       cfg_.order_qty, ahead, std::nullopt});
        active_.push_back(ActiveOrder{next_id_++, pair_id, /*is_naive=*/false, key, msg_idx, bid, ask,
                                       cfg_.order_qty, ahead,
                                       FastAheadTracker(key, our_seq, ahead)});
    }

    void finish(std::vector<SimOrderResult>& results, const ActiveOrder& ao, Outcome outcome, uint64_t msg_idx) {
        SimOrderResult r;
        r.id = ao.id;
        r.pair_id = ao.pair_id;
        r.is_naive = ao.is_naive;
        r.side = ao.level.side;
        r.price_ticks = ao.level.price_ticks;
        r.qty = ao.qty;
        r.placed_msg_idx = ao.placed_msg_idx;
        r.resolved_msg_idx = msg_idx;
        r.outcome = outcome;
        r.bid_at_place = ao.bid_at_place;
        r.ask_at_place = ao.ask_at_place;
        r.bid_at_fill = book_.has_bid() ? book_.best_bid_ticks() : ao.bid_at_place;
        r.ask_at_fill = book_.has_ask() ? book_.best_ask_ticks() : ao.ask_at_place;
        r.ahead_qty_at_place = ao.ahead_qty_at_place;
        results.push_back(r);
    }

    Config cfg_;
    OrderBook book_;
    std::unordered_map<uint64_t, RestingLoc> book_orders_;
    QueuePositionBook qbook_;
    std::vector<ActiveOrder> active_;
    uint64_t next_id_ = 1;
    uint64_t next_pair_id_ = 1;
};

}  // namespace mdfeed
