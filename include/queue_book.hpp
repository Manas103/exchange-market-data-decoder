// Queue-position tracking layer, built on top of the decoder's own wire
// protocol (protocol.hpp).
//
// The base repo's OrderBook tracks aggregated quantity per price level only.
// Nothing in the original repo knows the arrival order of individual resting
// orders within a level, which is what "queue position" means: whether a
// resting order is the 1st, 50th or 500th order deep at its price, and how
// that position ages as orders ahead of it cancel or trade away. This header
// adds exactly that, driven by literally the same WireMsg stream the live
// decoder consumes, nothing else.
//
// Two ways to compute how much quantity sits ahead of a hypothetical resting
// order at a given (instrument, side, price):
//   - QueuePositionBook::qty_ahead_of_seq: the reference oracle. Recomputes
//     from scratch by scanning the level's live FIFO list. O(level size),
//     deliberately slow and obviously correct by construction: it just sums
//     the quantity of every entry that arrived before our hypothetical order.
//   - FastAheadTracker: the incremental version actually used by the
//     backtest. O(1) per relevant message: it only inspects the single
//     ReductionEvent produced by whichever message was just applied, and
//     only acts on it if the affected entry arrived before ours. This is
//     what makes a multi-million-message backtest replay tractable.
// tests/queue_position_reference_test.cpp diffs the two against each other
// over a long replay; that diff is the correctness evidence for this file.
#pragma once
#include <algorithm>
#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>
#include <vector>

#include "protocol.hpp"

namespace mdfeed {

struct LevelKey {
    uint16_t instrument;
    Side side;
    int64_t price_ticks;
    bool operator==(const LevelKey& o) const {
        return instrument == o.instrument && side == o.side && price_ticks == o.price_ticks;
    }
};

struct LevelKeyHash {
    size_t operator()(const LevelKey& k) const noexcept {
        uint64_t h = (static_cast<uint64_t>(k.instrument) << 1) | (k.side == Side::Buy ? 0u : 1u);
        uint64_t p = static_cast<uint64_t>(k.price_ticks);
        h ^= p + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<size_t>(h);
    }
};

// One resting order's slot in a level's FIFO queue. `seq` is a globally
// increasing arrival counter assigned once, at insertion; it is the only
// thing that defines "ahead of" or "behind" between two entries, including
// entries at different instruments/prices (a lower seq always means the
// order arrived earlier in the replayed stream than a higher one).
struct QEntry {
    uint64_t order_id;
    uint32_t qty;
    uint64_t seq;
};

// Reported effect of applying one message: which level(s) it touched, and,
// if it reduced or removed a resting order (Execute, Cancel, Delete, or the
// old-order side of a Replace), exactly which entry and by how much.
// `is_execute` distinguishes a real trade (Execute) from a cancel/delete/
// replace-out; a fill can only be triggered by a real trade, never by a
// cancel, even though a cancel ahead of a tracked order does advance it.
struct ReductionEvent {
    LevelKey level;
    uint64_t entry_seq;
    uint32_t qty_reduced;
    bool is_execute;
};

struct ApplyEffect {
    std::vector<LevelKey> touched;
    std::optional<ReductionEvent> reduction;
};

// Ground-truth FIFO queue-position book. Deliberately single-threaded: a
// backtest replay is meant to be deterministic, and the live decoder's
// sharded/threaded Normalizer exists to hide network and syscall latency
// during live ingestion, which is not a concern a backtest replay has. See
// README, "Honest framing", for why this is a separate, smaller class
// rather than a thread-safety retrofit of Normalizer.
class QueuePositionBook {
public:
    struct Loc {
        LevelKey level;
        std::list<QEntry>::iterator it;
    };

    ApplyEffect apply(const WireMsg& m) {
        ApplyEffect eff;
        switch (m.header.type) {
            case MsgType::AddOrder: {
                const auto& a = m.add;
                LevelKey key{a.header.instrument_id, a.side, a.price_ticks};
                auto& lst = levels_[key];
                lst.push_back(QEntry{a.order_id, a.qty, next_seq_++});
                orders_[a.order_id] = Loc{key, std::prev(lst.end())};
                eff.touched.push_back(key);
                break;
            }
            case MsgType::Execute:
                reduce_by(m.exec.order_id, m.exec.exec_qty, /*is_execute=*/true, eff);
                break;
            case MsgType::Cancel:
                reduce_by(m.cancel.order_id, m.cancel.cancel_qty, /*is_execute=*/false, eff);
                break;
            case MsgType::Delete: {
                auto oit = orders_.find(m.del.order_id);
                if (oit != orders_.end()) {
                    reduce_by(m.del.order_id, oit->second.it->qty, /*is_execute=*/false, eff);
                }
                break;
            }
            case MsgType::Replace: {
                const auto& r = m.replace;
                auto oit = orders_.find(r.old_order_id);
                if (oit == orders_.end()) break;  // unknown id: dropped, matches Normalizer::apply
                Side side = oit->second.level.side;
                reduce_by(r.old_order_id, oit->second.it->qty, /*is_execute=*/false, eff);
                LevelKey key{r.header.instrument_id, side, r.price_ticks};
                auto& lst = levels_[key];
                lst.push_back(QEntry{r.new_order_id, r.qty, next_seq_++});
                orders_[r.new_order_id] = Loc{key, std::prev(lst.end())};
                eff.touched.push_back(key);
                break;
            }
        }
        return eff;
    }

    uint64_t current_seq() const { return next_seq_; }

    uint32_t level_total_qty(const LevelKey& key) const {
        auto it = levels_.find(key);
        if (it == levels_.end()) return 0;
        uint64_t total = 0;
        for (const auto& e : it->second) total += e.qty;
        return static_cast<uint32_t>(total);
    }

    // Reference oracle. See file header.
    uint64_t qty_ahead_of_seq(const LevelKey& key, uint64_t seq_threshold) const {
        auto it = levels_.find(key);
        if (it == levels_.end()) return 0;
        uint64_t total = 0;
        for (const auto& e : it->second)
            if (e.seq < seq_threshold) total += e.qty;
        return total;
    }

    // Every order currently resting anywhere, for conservation checks.
    uint64_t total_resting_qty() const {
        uint64_t total = 0;
        for (const auto& [key, lst] : levels_)
            for (const auto& e : lst) total += e.qty;
        return total;
    }
    size_t live_order_count() const { return orders_.size(); }

private:
    void reduce_by(uint64_t order_id, uint32_t qty_to_remove, bool is_execute, ApplyEffect& eff) {
        auto oit = orders_.find(order_id);
        if (oit == orders_.end()) return;  // unknown order id: dropped, matches Normalizer::apply
        Loc loc = oit->second;
        uint32_t actual = std::min(qty_to_remove, loc.it->qty);
        eff.reduction = ReductionEvent{loc.level, loc.it->seq, actual, is_execute};
        eff.touched.push_back(loc.level);
        loc.it->qty -= actual;
        if (loc.it->qty == 0) {
            auto lvl_it = levels_.find(loc.level);
            lvl_it->second.erase(loc.it);
            if (lvl_it->second.empty()) levels_.erase(lvl_it);
            orders_.erase(oit);
        }
    }

    std::unordered_map<LevelKey, std::list<QEntry>, LevelKeyHash> levels_;
    std::unordered_map<uint64_t, Loc> orders_;
    uint64_t next_seq_ = 0;
};

// Incremental ("fast") tracker for one hypothetical resting order's queue
// position. O(1) per message that touches its level, versus the O(level
// size) reference oracle above. This is the production code path; the
// reference test diffs its `ahead_qty()` against
// QueuePositionBook::qty_ahead_of_seq after every touching message over a
// long replay.
//
// Aging rule (the part the resume claim is about): an entry that arrived
// before our order (`entry_seq < our_seq_`) advances us when it is reduced,
// whether by a cancel, a delete, or an execute -- all three remove
// competing quantity from ahead of us. An entry that arrived after our
// order (`entry_seq >= our_seq_`) never advances us, no matter what happens
// to it, because it is not ahead of us in the first place. A *fill* is
// stricter still: it requires our position to already be at the front
// (ahead_qty == 0, as of a strictly earlier message) AND a real Execute,
// not a cancel, on this message. Reaching the front and the trade that
// fills us are never the same message; real matching does not let a
// still-ahead order both clear out of the way and be traded through in one
// event.
class FastAheadTracker {
public:
    FastAheadTracker(LevelKey level, uint64_t our_seq, uint64_t ahead_qty_at_insert)
        : level_(level), our_seq_(our_seq), ahead_qty_(static_cast<int64_t>(ahead_qty_at_insert)) {
        if (ahead_qty_ <= 0) {
            ahead_qty_ = 0;
            became_front_ = true;
        }
    }

    // Feed the effect of one applied message. Returns true if this message
    // is a genuine fill trigger for the order this tracker represents.
    bool consume(const ApplyEffect& eff) {
        if (!eff.reduction) return false;
        const ReductionEvent& r = *eff.reduction;
        if (!(r.level == level_)) return false;
        const bool was_front = became_front_;
        if (r.entry_seq < our_seq_) {
            ahead_qty_ -= static_cast<int64_t>(r.qty_reduced);
            if (ahead_qty_ < 0) ahead_qty_ = 0;
            if (ahead_qty_ == 0) became_front_ = true;
        }
        return was_front && r.is_execute;
    }

    int64_t ahead_qty() const { return ahead_qty_; }
    bool at_front() const { return became_front_; }
    const LevelKey& level() const { return level_; }
    uint64_t seq() const { return our_seq_; }

private:
    LevelKey level_;
    uint64_t our_seq_;
    int64_t ahead_qty_;
    bool became_front_ = false;
};

}  // namespace mdfeed
