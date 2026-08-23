// Per-instrument limit order book.
//
// Design choice: aggregated price levels are stored in a flat array indexed
// by tick offset from a per-instrument base price, not a std::map<price,qty>.
// Real order books stay within a bounded band around the last trade price
// (exchanges halt or widen limits long before price moves thousands of
// ticks in a session), so a fixed-size array gives O(1) level updates
// instead of O(log n) tree operations -- the standard trick used in most
// "fast limit order book" writeups. The tradeoff, stated plainly: a price
// that moves outside the pre-sized band is a bug, not a graceful path, so
// the level index is bounds-checked and out-of-range updates are dropped
// with a counter rather than corrupting memory or throwing mid-feed.
//
// Individual resting orders (needed so a Cancel/Delete/Replace referencing
// an order_id can find and adjust the right level in O(1)) live in a
// per-shard hash map owned by the caller (see normalizer.hpp), not here --
// this class only tracks aggregated qty-per-level and top of book.
#pragma once
#include <array>
#include <cstdint>
#include <algorithm>
#include "protocol.hpp"

namespace mdfeed {

class OrderBook {
public:
    static constexpr int kLevels = 16384;
    static constexpr int kHalf = kLevels / 2;

    void init(int64_t reference_price_ticks) {
        base_price_ = reference_price_ticks - kHalf;
        bid_qty_.fill(0);
        ask_qty_.fill(0);
        best_bid_idx_ = -1;
        best_ask_idx_ = kLevels;
    }

    // Returns false (and bumps dropped_) if price_ticks falls outside the
    // pre-sized band around this instrument's reference price.
    bool add(Side side, int64_t price_ticks, uint32_t qty) {
        int idx = index_of(price_ticks);
        if (idx < 0 || idx >= kLevels) { ++dropped_; return false; }
        if (side == Side::Buy) {
            bid_qty_[idx] += qty;
            if (idx > best_bid_idx_) best_bid_idx_ = idx;
        } else {
            ask_qty_[idx] += qty;
            if (idx < best_ask_idx_) best_ask_idx_ = idx;
        }
        return true;
    }

    bool reduce(Side side, int64_t price_ticks, uint32_t qty) {
        int idx = index_of(price_ticks);
        if (idx < 0 || idx >= kLevels) { ++dropped_; return false; }
        if (side == Side::Buy) {
            int64_t& q = bid_qty_[idx];
            q -= qty;
            if (q < 0) q = 0;
            if (idx == best_bid_idx_ && q == 0) {
                while (best_bid_idx_ >= 0 && bid_qty_[best_bid_idx_] == 0) --best_bid_idx_;
            }
        } else {
            int64_t& q = ask_qty_[idx];
            q -= qty;
            if (q < 0) q = 0;
            if (idx == best_ask_idx_ && q == 0) {
                while (best_ask_idx_ < kLevels && ask_qty_[best_ask_idx_] == 0) ++best_ask_idx_;
            }
        }
        return true;
    }

    bool has_bid() const { return best_bid_idx_ >= 0; }
    bool has_ask() const { return best_ask_idx_ < kLevels; }
    int64_t best_bid_ticks() const { return base_price_ + best_bid_idx_; }
    int64_t best_ask_ticks() const { return base_price_ + best_ask_idx_; }
    int64_t best_bid_qty() const { return has_bid() ? bid_qty_[best_bid_idx_] : 0; }
    int64_t best_ask_qty() const { return has_ask() ? ask_qty_[best_ask_idx_] : 0; }
    uint64_t dropped() const { return dropped_; }

    // Raw contiguous level arrays. Added for the Python binding: nanobind
    // wraps these pointers in a NumPy array whose buffer IS this book's
    // storage, so Python reads the live aggregated depth with no copy and no
    // per-call allocation. Exposing the pointer is the whole point; the
    // lifetime contract (the view is valid only while the owning feed object
    // is alive) is documented on the Python side and enforced by nanobind's
    // owner keep-alive.
    const int64_t* bid_data() const { return bid_qty_.data(); }
    const int64_t* ask_data() const { return ask_qty_.data(); }
    static constexpr int levels() { return kLevels; }
    int64_t base_price_ticks() const { return base_price_; }
    int best_bid_index() const { return best_bid_idx_; }
    int best_ask_index() const { return best_ask_idx_; }

    int64_t qty_at(Side side, int64_t price_ticks) const {
        int idx = index_of(price_ticks);
        if (idx < 0 || idx >= kLevels) return 0;
        return side == Side::Buy ? bid_qty_[idx] : ask_qty_[idx];
    }

private:
    int index_of(int64_t price_ticks) const {
        return static_cast<int>(price_ticks - base_price_);
    }

    int64_t base_price_ = 0;
    std::array<int64_t, kLevels> bid_qty_{};
    std::array<int64_t, kLevels> ask_qty_{};
    int best_bid_idx_ = -1;
    int best_ask_idx_ = kLevels;
    uint64_t dropped_ = 0;
};

} // namespace mdfeed
