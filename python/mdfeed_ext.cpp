// nanobind bindings: full-depth books, live off a multicast socket, readable
// from Python with no copy.
//
// The whole point of this file is what it does NOT do. Every array-returning
// function here hands NumPy a pointer that already exists inside the C++
// decoder (an OrderBook's flat level array, or the receiver's latency vector)
// and wraps it in an nb::ndarray with an owner object. No memcpy, no
// allocation, no per-call marshalling. `bid_depth(i)` on a 16,384-level book
// costs the same whether the book holds one resting order or a million.
//
// Two things make that safe rather than reckless:
//
//   1. Lifetime. Each ndarray is constructed with the owning Python object as
//      its `owner`, so nanobind keeps the LiveFeed or OfflineBooks alive for
//      as long as any view into it survives. Dropping the feed while holding
//      an array does not dangle.
//   2. Mutability. The arrays are handed out with a const scalar type, which
//      nanobind surfaces as a non-writeable NumPy array. Python can
//      read live depth; it cannot corrupt a book the decoder threads own.
//
// The honest caveat, stated here and in the README rather than buried: while
// the feed is running, shard threads write those same int64 slots with no
// synchronization against the Python reader. On x86_64 an aligned 8-byte load
// will not tear, so a read during the run yields some real level quantity from
// some point in the recent past, not garbage. It is still a data race by the
// letter of the C++ memory model. Exact reads are the ones taken after wait(),
// when the shards are joined and the books are quiescent, and that is where
// every verified number below comes from.
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/string.h>

#include <cstdint>
#include <stdexcept>
#include <string>

#include "live_feed.hpp"

namespace nb = nanobind;
using namespace mdfeed;

namespace {

using LevelArray = nb::ndarray<nb::numpy, const int64_t, nb::ndim<1>, nb::c_contig>;
using U64Array = nb::ndarray<nb::numpy, const uint64_t, nb::ndim<1>, nb::c_contig>;

// One helper so there is exactly one place where a pointer becomes an array.
LevelArray wrap_levels(const int64_t* data, size_t n, nb::handle owner) {
    return LevelArray(data, {n}, owner);
}

void check_instrument(int instrument, int n) {
    if (instrument < 0 || instrument >= n) {
        throw std::out_of_range("instrument id out of range");
    }
}

} // namespace

NB_MODULE(mdfeed_ext, m) {
    m.doc() =
        "Zero-copy Python view onto an ITCH-style multicast decoder's full-depth order books.\n"
        "Arrays returned by this module alias the decoder's own memory; they are read-only\n"
        "and keep their owning feed alive.";

    m.attr("NUM_INSTRUMENTS") = NUM_INSTRUMENTS;
    m.attr("LEVELS_PER_SIDE") = OrderBook::levels();
    m.attr("PRICE_TICK") = PRICE_TICK;

    nb::class_<FeedStats>(m, "FeedStats")
        .def_ro("sent", &FeedStats::sent)
        .def_ro("received", &FeedStats::received)
        .def_ro("datagrams", &FeedStats::datagrams)
        .def_ro("seconds", &FeedStats::seconds)
        .def_ro("loss_pct", &FeedStats::loss_pct)
        .def_ro("msgs_per_sec", &FeedStats::msgs_per_sec)
        .def_ro("msgs_per_datagram", &FeedStats::msgs_per_datagram)
        .def("__repr__", [](const FeedStats& s) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "FeedStats(sent=%llu, received=%llu, datagrams=%llu, loss_pct=%.4f, "
                          "msgs_per_sec=%.0f)",
                          (unsigned long long)s.sent, (unsigned long long)s.received,
                          (unsigned long long)s.datagrams, s.loss_pct, s.msgs_per_sec);
            return std::string(buf);
        });

    nb::class_<LiveFeed>(m, "LiveFeed")
        .def(nb::init<int, const std::string&, uint16_t, size_t>(), nb::arg("num_shards") = 4,
             nb::arg("group") = "239.255.10.10", nb::arg("port") = (uint16_t)30001,
             nb::arg("capacity") = (size_t)2'000'000,
             "Join the multicast group and stand up `num_shards` normalizer threads. "
             "`capacity` sizes the latency vector up front so the receive path never "
             "reallocates mid-run.")
        // GIL released for start/wait: the C++ threads must run while Python is
        // free to hold arrays and poll progress.
        .def("start", &LiveFeed::start, nb::arg("num_messages"),
             nb::call_guard<nb::gil_scoped_release>(),
             "Spawn the publisher and receiver threads and return immediately.")
        .def("wait", &LiveFeed::wait, nb::call_guard<nb::gil_scoped_release>(),
             "Block until the publisher finishes and every shard queue drains. "
             "After this returns the books are quiescent and reads are exact.")
        .def_prop_ro("running", &LiveFeed::running)
        .def_prop_ro("received_so_far", &LiveFeed::received_so_far,
                     "Messages decoded so far. Safe to poll while the feed runs.")
        .def_prop_ro("stats", &LiveFeed::stats, nb::rv_policy::reference_internal)
        .def_prop_ro("num_instruments", &LiveFeed::num_instruments)
        .def("reference_price_ticks", &LiveFeed::reference_price_ticks, nb::arg("instrument"))
        .def(
            "bid_depth",
            [](nb::object self, int instrument) {
                LiveFeed& f = nb::cast<LiveFeed&>(self);
                check_instrument(instrument, f.num_instruments());
                const OrderBook& b = f.book(static_cast<uint16_t>(instrument));
                return wrap_levels(b.bid_data(), OrderBook::levels(), self);
            },
            nb::arg("instrument"),
            "Read-only NumPy view of this instrument's aggregated bid quantity per price "
            "level. Aliases the decoder's array; no copy is made.")
        .def(
            "ask_depth",
            [](nb::object self, int instrument) {
                LiveFeed& f = nb::cast<LiveFeed&>(self);
                check_instrument(instrument, f.num_instruments());
                const OrderBook& b = f.book(static_cast<uint16_t>(instrument));
                return wrap_levels(b.ask_data(), OrderBook::levels(), self);
            },
            nb::arg("instrument"), "Read-only NumPy view of aggregated ask quantity per level.")
        .def(
            "latencies_ns",
            [](nb::object self) {
                LiveFeed& f = nb::cast<LiveFeed&>(self);
                const auto& v = f.latencies();
                return U64Array(v.data(), {v.size()}, self);
            },
            "Read-only NumPy view of every send-to-decode latency sample. A 2M-sample run "
            "costs 16 MB in C++ and zero extra bytes to look at from Python.")
        .def(
            "depth_ptr",
            [](LiveFeed& f, int instrument, bool ask) {
                check_instrument(instrument, f.num_instruments());
                const OrderBook& b = f.book(static_cast<uint16_t>(instrument));
                return reinterpret_cast<uintptr_t>(ask ? b.ask_data() : b.bid_data());
            },
            nb::arg("instrument"), nb::arg("ask") = false,
            "Raw address of the level array. Exposed so the test suite can assert that the "
            "NumPy buffer address equals it, which is the only way to prove no copy happened.")
        .def("base_price_ticks",
             [](LiveFeed& f, int instrument) {
                 check_instrument(instrument, f.num_instruments());
                 return f.book(static_cast<uint16_t>(instrument)).base_price_ticks();
             })
        .def("best_bid_ticks",
             [](LiveFeed& f, int instrument) -> nb::object {
                 const OrderBook& b = f.book(static_cast<uint16_t>(instrument));
                 if (!b.has_bid()) return nb::none();
                 return nb::cast(b.best_bid_ticks());
             })
        .def("best_ask_ticks", [](LiveFeed& f, int instrument) -> nb::object {
            const OrderBook& b = f.book(static_cast<uint16_t>(instrument));
            if (!b.has_ask()) return nb::none();
            return nb::cast(b.best_ask_ticks());
        });

    nb::class_<OfflineBooks>(m, "OfflineBooks")
        .def(nb::init<size_t, int, uint32_t>(), nb::arg("num_messages"), nb::arg("num_shards"),
             nb::arg("seed") = (uint32_t)7, nb::call_guard<nb::gil_scoped_release>(),
             "Replay `num_messages` of the synthetic feed through an `num_shards`-shard "
             "normalizer with no sockets involved. Construct one at 1 shard and one at 8 and "
             "diff their depth views to reproduce the reference-build check in NumPy.")
        .def_prop_ro("num_instruments", &OfflineBooks::num_instruments)
        .def("reference_price_ticks", &OfflineBooks::reference_price_ticks, nb::arg("instrument"))
        .def(
            "bid_depth",
            [](nb::object self, int instrument) {
                OfflineBooks& o = nb::cast<OfflineBooks&>(self);
                check_instrument(instrument, o.num_instruments());
                return wrap_levels(o.book(static_cast<uint16_t>(instrument)).bid_data(),
                                   OrderBook::levels(), self);
            },
            nb::arg("instrument"))
        .def(
            "ask_depth",
            [](nb::object self, int instrument) {
                OfflineBooks& o = nb::cast<OfflineBooks&>(self);
                check_instrument(instrument, o.num_instruments());
                return wrap_levels(o.book(static_cast<uint16_t>(instrument)).ask_data(),
                                   OrderBook::levels(), self);
            },
            nb::arg("instrument"))
        .def("depth_ptr",
             [](OfflineBooks& o, int instrument, bool ask) {
                 check_instrument(instrument, o.num_instruments());
                 const OrderBook& b = o.book(static_cast<uint16_t>(instrument));
                 return reinterpret_cast<uintptr_t>(ask ? b.ask_data() : b.bid_data());
             },
             nb::arg("instrument"), nb::arg("ask") = false)
        .def("base_price_ticks",
             [](OfflineBooks& o, int instrument) {
                 check_instrument(instrument, o.num_instruments());
                 return o.book(static_cast<uint16_t>(instrument)).base_price_ticks();
             })
        .def("best_bid_ticks",
             [](OfflineBooks& o, int instrument) -> nb::object {
                 const OrderBook& b = o.book(static_cast<uint16_t>(instrument));
                 if (!b.has_bid()) return nb::none();
                 return nb::cast(b.best_bid_ticks());
             })
        .def("best_ask_ticks", [](OfflineBooks& o, int instrument) -> nb::object {
            const OrderBook& b = o.book(static_cast<uint16_t>(instrument));
            if (!b.has_ask()) return nb::none();
            return nb::cast(b.best_ask_ticks());
        });
}
