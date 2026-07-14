// Lock-free single-producer/single-consumer ring buffer.
//
// One dispatcher thread (the "producer") reads the wire and pushes decoded
// messages into one of these per worker-thread shard; each worker thread is
// the sole "consumer" of its own buffer. That single-producer/single-
// consumer contract is what makes this safe with plain atomics and no
// locks or CAS loops -- it is not a general MPMC queue, and using it from
// more than one producer or consumer thread is undefined behavior.
#pragma once
#include <atomic>
#include <array>
#include <cstddef>

namespace mdfeed {

template <typename T, size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

public:
    bool push(const T& item) noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & kMask;
        if (next == tail_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[head] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& out) noexcept {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false; // empty
        }
        out = buffer_[tail];
        tail_.store((tail + 1) & kMask, std::memory_order_release);
        return true;
    }

    size_t size_approx() const noexcept {
        const size_t h = head_.load(std::memory_order_acquire);
        const size_t t = tail_.load(std::memory_order_acquire);
        return (h - t) & kMask;
    }

private:
    static constexpr size_t kMask = Capacity - 1;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    std::array<T, Capacity> buffer_{};
};

} // namespace mdfeed
