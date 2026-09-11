// Single-venue simulated feed: one synthetic order-flow generator, replayed
// into one full-depth OrderBook, the way the base decoder/normalizer
// machinery already does it (see normalizer.hpp::apply), but single-threaded
// and single-instrument, because a venue in the cross-venue NBBO layer needs
// exactly one book, not 512 sharded across worker threads.
//
// This intentionally reuses OrderBook and MarketSimulator completely
// unchanged; the only new code below is the order_id -> location bookkeeping
// a standalone book needs to resolve Cancel/Delete/Replace, which
// normalizer.hpp already carries on its Shard struct and
// tests/queue_position_reference_test.cpp already duplicates in miniature
// for the identical single-instrument reason (a backtest replay, like a
// simulated venue, is deterministic offline work, not live multi-instrument
// ingestion under network and syscall latency, so the sharded/threaded
// Normalizer is the wrong tool here, not a missing feature).
#pragma once
#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "market_simulator.hpp"
#include "order_book.hpp"
#include "protocol.hpp"

namespace mdfeed {

// One simulated trading venue quoting a single symbol: its own independent
// order-flow generator (a different seed than its sibling venues, so its
// book evolves independently, the way three real venues' matching engines
// are never in lockstep) driving its own full-depth OrderBook.
class VenueFeed {
public:
    VenueFeed(int venue_id, uint32_t seed, int64_t reference_price_ticks)
        : venue_id_(venue_id), sim_(seed, /*num_instruments=*/1) {
        book_.init(reference_price_ticks);
    }

    int venue_id() const { return venue_id_; }
    const OrderBook& book() const { return book_; }
    uint64_t dropped() const { return book_.dropped(); }

    // Pull and apply exactly one message from this venue's own independent
    // order flow. Returns the applied message so a caller can log or time
    // the specific event that changed this venue's book.
    WireMsg step() {
        WireMsg m = sim_.next_message();
        apply(m);
        return m;
    }

private:
    struct Loc {
        Side side;
        int64_t price_ticks;
        uint32_t qty;
    };

    // Every m.<variant>.order_id read below is copied into a local uint64_t
    // before use, rather than passed straight through as a reference into
    // unordered_map's operator[]/find. WireMsg's variants are
    // #pragma pack(1) (protocol.hpp), which is correct for the wire format
    // but means a uint64_t field can land at a misaligned offset inside the
    // union; binding a reference straight to it (which operator[]/find do
    // internally) is undefined behavior even though x86 tolerates the actual
    // load. UBSan (see docs/nbbo_reference_test_san.txt) caught this on the
    // very first sanitizer run of this new file: "reference binding to
    // misaligned address ... which requires 8 byte alignment", inside
    // VenueFeed::apply by way of unordered_map::operator[]. The fix is the
    // copy below, not a protocol change; the same access pattern already
    // exists in the base repo's normalizer.hpp for the identical reason and
    // is out of scope for this extension, but is worth knowing about there
    // too.
    void apply(const WireMsg& m) {
        switch (m.header.type) {
            case MsgType::AddOrder: {
                const auto& a = m.add;
                const uint64_t order_id = a.order_id;
                orders_[order_id] = Loc{a.side, a.price_ticks, a.qty};
                book_.add(a.side, a.price_ticks, a.qty);
                break;
            }
            case MsgType::Execute: {
                const uint64_t order_id = m.exec.order_id;
                auto it = orders_.find(order_id);
                if (it == orders_.end()) break;
                const uint32_t exec_qty = m.exec.exec_qty;
                uint32_t rq = std::min(exec_qty, it->second.qty);
                book_.reduce(it->second.side, it->second.price_ticks, rq);
                it->second.qty -= rq;
                if (it->second.qty == 0) orders_.erase(it);
                break;
            }
            case MsgType::Cancel: {
                const uint64_t order_id = m.cancel.order_id;
                auto it = orders_.find(order_id);
                if (it == orders_.end()) break;
                const uint32_t cancel_qty = m.cancel.cancel_qty;
                uint32_t rq = std::min(cancel_qty, it->second.qty);
                book_.reduce(it->second.side, it->second.price_ticks, rq);
                it->second.qty -= rq;
                if (it->second.qty == 0) orders_.erase(it);
                break;
            }
            case MsgType::Delete: {
                const uint64_t order_id = m.del.order_id;
                auto it = orders_.find(order_id);
                if (it == orders_.end()) break;
                book_.reduce(it->second.side, it->second.price_ticks, it->second.qty);
                orders_.erase(it);
                break;
            }
            case MsgType::Replace: {
                const uint64_t old_order_id = m.replace.old_order_id;
                auto it = orders_.find(old_order_id);
                if (it == orders_.end()) break;
                Side side = it->second.side;
                book_.reduce(side, it->second.price_ticks, it->second.qty);
                orders_.erase(it);
                const uint64_t new_order_id = m.replace.new_order_id;
                orders_[new_order_id] = Loc{side, m.replace.price_ticks, m.replace.qty};
                book_.add(side, m.replace.price_ticks, m.replace.qty);
                break;
            }
        }
    }

    int venue_id_;
    MarketSimulator sim_;
    OrderBook book_;
    std::unordered_map<uint64_t, Loc> orders_;
};

}  // namespace mdfeed
