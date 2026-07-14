// Synthetic order-flow generator.
//
// Produces a *self-consistent* stream of Add/Execute/Cancel/Delete/Replace
// messages across `num_instruments` instruments: it tracks which order_ids
// are currently "live" per instrument so every Execute/Cancel/Delete/Replace
// references a real resting order with enough quantity, the same way real
// exchange feeds never reference a nonexistent order. Reference prices per
// instrument drift slowly over time so resting orders cluster near a moving
// touch rather than a static price.
//
// This class does NOT maintain its own order book -- it only needs to know
// which orders exist to generate valid messages. Ground truth for
// correctness testing comes from replaying the message stream through a
// single-shard (single-threaded) Normalizer, not from a second book kept
// here (see tests/correctness_test.cpp).
#pragma once
#include <cstdint>
#include <random>
#include <vector>
#include "protocol.hpp"

namespace mdfeed {

struct LiveOrder {
    uint64_t order_id;
    Side side;
    int64_t price_ticks;
    uint32_t qty;
};

class MarketSimulator {
public:
    explicit MarketSimulator(uint32_t seed, int num_instruments = NUM_INSTRUMENTS)
        : rng_(seed), num_instruments_(num_instruments) {
        ref_price_.resize(num_instruments);
        live_orders_.resize(num_instruments);
        std::uniform_real_distribution<double> price_dist(20.0, 400.0);
        for (int i = 0; i < num_instruments; ++i) {
            ref_price_[i] = static_cast<int64_t>(price_dist(rng_) * 100.0);
        }
    }

    int num_instruments() const { return num_instruments_; }
    int64_t reference_price_ticks(int instrument) const { return ref_price_[instrument]; }

    WireMsg next_message() {
        std::uniform_int_distribution<int> instr_dist(0, num_instruments_ - 1);
        const int instr = instr_dist(rng_);
        auto& book = live_orders_[instr];

        if (drift_dist_(rng_) < 0.0005) {
            std::uniform_int_distribution<int> drift(-3, 3);
            ref_price_[instr] += drift(rng_);
        }

        WireMsg m{};
        const double roll = action_dist_(rng_);

        if (book.empty() || roll < 0.55) {
            m.add.header.type = MsgType::AddOrder;
            m.add.header.instrument_id = static_cast<uint16_t>(instr);
            m.add.header.timestamp_ns = 0;
            m.add.order_id = next_order_id_++;
            m.add.side = (action_dist_(rng_) < 0.5) ? Side::Buy : Side::Sell;
            std::uniform_int_distribution<int> offset_dist(1, 200);
            const int off = offset_dist(rng_);
            m.add.price_ticks = ref_price_[instr] + (m.add.side == Side::Buy ? -off : off);
            std::uniform_int_distribution<int> qty_dist(1, 500);
            m.add.qty = static_cast<uint32_t>(qty_dist(rng_));
            book.push_back({m.add.order_id, m.add.side, m.add.price_ticks, m.add.qty});
            return m;
        }

        std::uniform_int_distribution<size_t> pick_dist(0, book.size() - 1);
        const size_t idx = pick_dist(rng_);
        LiveOrder lo = book[idx];
        const double sub_roll = action_dist_(rng_);

        if (sub_roll < 0.40) {
            m.exec.header.type = MsgType::Execute;
            m.exec.header.instrument_id = static_cast<uint16_t>(instr);
            m.exec.header.timestamp_ns = 0;
            m.exec.order_id = lo.order_id;
            std::uniform_int_distribution<uint32_t> exec_qty_dist(1, lo.qty);
            m.exec.exec_qty = exec_qty_dist(rng_);
            lo.qty -= m.exec.exec_qty;
        } else if (sub_roll < 0.65) {
            m.cancel.header.type = MsgType::Cancel;
            m.cancel.header.instrument_id = static_cast<uint16_t>(instr);
            m.cancel.header.timestamp_ns = 0;
            m.cancel.order_id = lo.order_id;
            std::uniform_int_distribution<uint32_t> cancel_qty_dist(1, lo.qty);
            m.cancel.cancel_qty = cancel_qty_dist(rng_);
            lo.qty -= m.cancel.cancel_qty;
        } else if (sub_roll < 0.85) {
            m.del.header.type = MsgType::Delete;
            m.del.header.instrument_id = static_cast<uint16_t>(instr);
            m.del.header.timestamp_ns = 0;
            m.del.order_id = lo.order_id;
            lo.qty = 0;
        } else {
            std::uniform_int_distribution<int> offset_dist(1, 200);
            const int off = offset_dist(rng_);
            std::uniform_int_distribution<int> qty_dist(1, 500);
            m.replace.header.type = MsgType::Replace;
            m.replace.header.instrument_id = static_cast<uint16_t>(instr);
            m.replace.header.timestamp_ns = 0;
            m.replace.old_order_id = lo.order_id;
            m.replace.new_order_id = next_order_id_++;
            m.replace.price_ticks = ref_price_[instr] + (lo.side == Side::Buy ? -off : off);
            m.replace.qty = static_cast<uint32_t>(qty_dist(rng_));
            lo.qty = 0;
            book[idx] = book.back();
            book.pop_back();
            book.push_back({m.replace.new_order_id, lo.side, m.replace.price_ticks, m.replace.qty});
            return m;
        }

        if (lo.qty == 0) {
            book[idx] = book.back();
            book.pop_back();
        } else {
            book[idx].qty = lo.qty;
        }
        return m;
    }

private:
    std::mt19937 rng_;
    int num_instruments_;
    std::vector<int64_t> ref_price_;
    std::vector<std::vector<LiveOrder>> live_orders_;
    uint64_t next_order_id_ = 1;
    std::uniform_real_distribution<double> action_dist_{0.0, 1.0};
    std::uniform_real_distribution<double> drift_dist_{0.0, 1.0};
};

} // namespace mdfeed
