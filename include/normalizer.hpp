// Multithreaded order-book normalizer.
//
// Instruments are sharded across worker threads by `instrument_id % num_shards`,
// so every message for a given instrument always lands on the same shard --
// that's what lets each shard own its books and its order_id -> location map
// without any cross-thread synchronization on the book state itself. The only
// synchronization in the hot path is the lock-free SPSC ring buffer feeding
// each shard from the single dispatcher thread (see ring_buffer.hpp).
#pragma once
#include <vector>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include "protocol.hpp"
#include "order_book.hpp"
#include "ring_buffer.hpp"

namespace mdfeed {

struct OrderLocation {
    uint16_t local_idx;
    Side side;
    int64_t price_ticks;
    uint32_t qty;
};

struct QueueItem {
    WireMsg msg;
    uint16_t local_idx;
    uint64_t dispatch_ts_ns;
};

constexpr size_t kQueueCapacity = 1u << 20; // 1,048,576 slots per shard

struct Shard {
    std::vector<OrderBook> books;
    std::unordered_map<uint64_t, OrderLocation> orders;
    SpscRingBuffer<QueueItem, kQueueCapacity> queue;
    std::vector<uint64_t> proc_latency_ns;
    std::vector<uint64_t> e2e_latency_ns;
    std::atomic<uint64_t> processed{0};
    std::atomic<uint64_t> dropped_full{0}; // messages dropped because queue was full
};

class Normalizer {
public:
    explicit Normalizer(int num_shards, size_t expected_msgs_per_shard = 2'000'000)
        : num_shards_(num_shards), shards_(num_shards), clock_start_(std::chrono::steady_clock::now()) {
        instrument_to_shard_.resize(NUM_INSTRUMENTS);
        instrument_to_local_.resize(NUM_INSTRUMENTS);
        for (int i = 0; i < NUM_INSTRUMENTS; ++i) {
            int shard = i % num_shards_;
            instrument_to_shard_[i] = static_cast<uint16_t>(shard);
            instrument_to_local_[i] = static_cast<uint16_t>(shards_[shard].books.size());
            shards_[shard].books.emplace_back();
        }
        for (auto& s : shards_) {
            s.proc_latency_ns.reserve(expected_msgs_per_shard);
            s.e2e_latency_ns.reserve(expected_msgs_per_shard);
        }
    }

    void init_reference_price(uint16_t instrument_id, int64_t reference_price_ticks) {
        int shard = instrument_to_shard_[instrument_id];
        int local = instrument_to_local_[instrument_id];
        shards_[shard].books[local].init(reference_price_ticks);
    }

    void start() {
        for (int i = 0; i < num_shards_; ++i) {
            workers_.emplace_back([this, i] { worker_loop(i); });
        }
    }

    // Called from the single dispatcher thread only.
    bool dispatch(const WireMsg& msg) {
        uint16_t instr = msg.header.instrument_id;
        int shard = instrument_to_shard_[instr];
        QueueItem item;
        item.msg = msg;
        item.local_idx = instrument_to_local_[instr];
        item.dispatch_ts_ns = now_ns();
        if (!shards_[shard].queue.push(item)) {
            shards_[shard].dropped_full.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    // Signal shutdown once all messages have been dispatched, then join.
    void drain_and_stop() {
        for (auto& s : shards_) {
            while (s.queue.size_approx() > 0) std::this_thread::yield();
        }
        stop_.store(true, std::memory_order_relaxed);
        for (auto& t : workers_) t.join();
    }

    int num_shards() const { return num_shards_; }
    Shard& shard(int i) { return shards_[i]; }
    const OrderBook& book_for_instrument(uint16_t instr) const {
        return shards_[instrument_to_shard_[instr]].books[instrument_to_local_[instr]];
    }

private:
    uint64_t now_ns() const {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now() - clock_start_)
            .count();
    }

    void apply(Shard& shard, const QueueItem& item) {
        const WireMsg& m = item.msg;
        switch (m.header.type) {
            case MsgType::AddOrder: {
                const AddOrderMsg& a = m.add;
                shard.orders[a.order_id] = OrderLocation{item.local_idx, a.side, a.price_ticks, a.qty};
                shard.books[item.local_idx].add(a.side, a.price_ticks, a.qty);
                break;
            }
            case MsgType::Execute: {
                const ExecuteMsg& e = m.exec;
                auto it = shard.orders.find(e.order_id);
                if (it == shard.orders.end()) break;
                uint32_t reduce_qty = std::min(e.exec_qty, it->second.qty);
                shard.books[item.local_idx].reduce(it->second.side, it->second.price_ticks, reduce_qty);
                it->second.qty -= reduce_qty;
                if (it->second.qty == 0) shard.orders.erase(it);
                break;
            }
            case MsgType::Cancel: {
                const CancelMsg& c = m.cancel;
                auto it = shard.orders.find(c.order_id);
                if (it == shard.orders.end()) break;
                uint32_t reduce_qty = std::min(c.cancel_qty, it->second.qty);
                shard.books[item.local_idx].reduce(it->second.side, it->second.price_ticks, reduce_qty);
                it->second.qty -= reduce_qty;
                if (it->second.qty == 0) shard.orders.erase(it);
                break;
            }
            case MsgType::Delete: {
                const DeleteMsg& d = m.del;
                auto it = shard.orders.find(d.order_id);
                if (it == shard.orders.end()) break;
                shard.books[item.local_idx].reduce(it->second.side, it->second.price_ticks, it->second.qty);
                shard.orders.erase(it);
                break;
            }
            case MsgType::Replace: {
                const ReplaceMsg& r = m.replace;
                auto it = shard.orders.find(r.old_order_id);
                if (it == shard.orders.end()) break;
                Side side = it->second.side;
                shard.books[item.local_idx].reduce(side, it->second.price_ticks, it->second.qty);
                shard.orders.erase(it);
                shard.orders[r.new_order_id] = OrderLocation{item.local_idx, side, r.price_ticks, r.qty};
                shard.books[item.local_idx].add(side, r.price_ticks, r.qty);
                break;
            }
        }
    }

    void worker_loop(int shard_idx) {
        Shard& shard = shards_[shard_idx];
        QueueItem item;
        while (true) {
            if (shard.queue.pop(item)) {
                auto t0 = std::chrono::steady_clock::now();
                apply(shard, item);
                auto t1 = std::chrono::steady_clock::now();
                uint64_t proc_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
                uint64_t completion_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - clock_start_).count();
                shard.proc_latency_ns.push_back(proc_ns);
                shard.e2e_latency_ns.push_back(completion_ns - item.dispatch_ts_ns);
                shard.processed.fetch_add(1, std::memory_order_relaxed);
            } else if (stop_.load(std::memory_order_relaxed)) {
                break;
            } else {
                std::this_thread::yield();
            }
        }
    }

    int num_shards_;
    std::vector<Shard> shards_;
    std::vector<uint16_t> instrument_to_shard_;
    std::vector<uint16_t> instrument_to_local_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    std::chrono::steady_clock::time_point clock_start_;
};

} // namespace mdfeed
