"""Does the Python side actually avoid a copy, and does it see a correct book?

Two questions, and they need different kinds of evidence.

"No copy" is not something a benchmark can prove. A fast copy is still a copy.
The only proof is address identity: the NumPy buffer address must equal the
address of the C++ level array. That is what `depth_ptr()` exists for, and the
first group of tests below is nothing but pointer arithmetic and lifetime.

"Correct book" is proved the same way the C++ suite proves it, by diffing
against a reference build, except the diff runs in NumPy over two zero-copy
views. Same oracle, different language, and it exercises the binding on the way
through.
"""

import gc

import numpy as np
import pytest

MESSAGES = 400_000
WINDOW = 300  # matches tests/correctness_test.cpp: ref-300 .. ref+300 inclusive


# --------------------------------------------------------------------------
# Group 1: the array is a view, not a copy
# --------------------------------------------------------------------------


def test_buffer_address_equals_cpp_array_address(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30011, 1024)
    for instrument in (0, 1, 255, 511):
        bid = feed.bid_depth(instrument)
        ask = feed.ask_depth(instrument)
        assert bid.__array_interface__["data"][0] == feed.depth_ptr(instrument, False)
        assert ask.__array_interface__["data"][0] == feed.depth_ptr(instrument, True)
        assert bid.shape == (mdfeed.LEVELS_PER_SIDE,)
        assert bid.dtype == np.int64


def test_repeated_calls_return_the_same_memory(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30012, 1024)
    first = feed.bid_depth(7)
    second = feed.bid_depth(7)
    assert first.__array_interface__["data"][0] == second.__array_interface__["data"][0]
    assert first is not second  # two Python objects, one buffer


def test_views_are_read_only(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30013, 1024)
    view = feed.bid_depth(3)
    assert view.flags["WRITEABLE"] is False
    with pytest.raises(ValueError):
        view[0] = 1


def test_view_keeps_the_feed_alive(mdfeed):
    """Dropping the feed while a view is outstanding must not dangle."""
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30014, 200_000)
    feed.start(50_000)
    feed.wait()
    view = feed.bid_depth(11)
    expected_sum = int(view.sum())
    del feed
    gc.collect()
    assert int(view.sum()) == expected_sum  # nanobind owner keep-alive held


def test_view_tracks_the_book_without_being_refetched(mdfeed):
    """The array is live: the decoder writes, Python reads, nobody re-marshals."""
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30015, 400_000)
    views = [feed.bid_depth(i) for i in range(feed.num_instruments)]
    before = sum(int(v.sum()) for v in views)
    assert before == 0
    feed.start(200_000)
    feed.wait()
    after = sum(int(v.sum()) for v in views)  # same array objects as before
    assert after > 0


def test_instrument_id_is_bounds_checked(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30016, 1024)
    for bad in (-1, mdfeed.NUM_INSTRUMENTS, 10_000):
        with pytest.raises(IndexError):
            feed.bid_depth(bad)


# --------------------------------------------------------------------------
# Group 2: the book Python sees is the right book
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def replay(mdfeed):
    """One shard versus eight, identical message stream. The reference build."""
    reference = mdfeed.OfflineBooks(MESSAGES, 1, 7)
    sharded = mdfeed.OfflineBooks(MESSAGES, 8, 7)
    return reference, sharded


def test_sharded_books_match_reference_over_the_cpp_window(replay):
    """The 307,712-level check from tests/correctness_test.cpp, run in NumPy."""
    reference, sharded = replay
    levels_checked = 0
    mismatches = 0
    for instrument in range(reference.num_instruments):
        ref_price = reference.reference_price_ticks(instrument)
        base = reference.base_price_ticks(instrument)
        lo = ref_price - WINDOW - base
        hi = ref_price + WINDOW - base + 1
        for side in ("bid_depth", "ask_depth"):
            a = getattr(reference, side)(instrument)[lo:hi]
            b = getattr(sharded, side)(instrument)[lo:hi]
            mismatches += int(np.count_nonzero(a != b))
        levels_checked += hi - lo
    assert levels_checked == 307_712, levels_checked
    assert mismatches == 0


def test_sharded_books_match_reference_over_full_depth(replay):
    """Wider than the C++ test: every level of every book, both sides."""
    reference, sharded = replay
    levels_checked = 0
    for instrument in range(reference.num_instruments):
        for side in ("bid_depth", "ask_depth"):
            a = getattr(reference, side)(instrument)
            b = getattr(sharded, side)(instrument)
            assert np.array_equal(a, b), f"instrument {instrument} {side}"
            levels_checked += a.size
    assert levels_checked == reference.num_instruments * 2 * 16384


def test_top_of_book_derived_in_numpy_matches_cpp(replay):
    """Cross-language oracle: NumPy recomputes what C++ tracked incrementally.

    C++ maintains best bid/ask as a running index updated on every event. NumPy
    recomputes it from scratch off the raw level array. They are two different
    algorithms over the same memory, so agreement across 512 instruments is a
    real check on the incremental bookkeeping, not a tautology.
    """
    reference, _ = replay
    checked = 0
    for instrument in range(reference.num_instruments):
        base = reference.base_price_ticks(instrument)

        bids = np.nonzero(reference.bid_depth(instrument))[0]
        expected_bid = int(base + bids.max()) if bids.size else None
        assert reference.best_bid_ticks(instrument) == expected_bid

        asks = np.nonzero(reference.ask_depth(instrument))[0]
        expected_ask = int(base + asks.min()) if asks.size else None
        assert reference.best_ask_ticks(instrument) == expected_ask
        checked += 1
    assert checked == reference.num_instruments


def _classify_touch(books):
    uncrossed = locked = crossed = empty = 0
    crossed_ids = []
    for instrument in range(books.num_instruments):
        bid = books.best_bid_ticks(instrument)
        ask = books.best_ask_ticks(instrument)
        if bid is None or ask is None:
            empty += 1
        elif bid < ask:
            uncrossed += 1
        elif bid == ask:
            locked += 1
            crossed_ids.append(instrument)
        else:
            crossed += 1
            crossed_ids.append(instrument)
    return uncrossed, locked, crossed, empty, crossed_ids


def test_locked_and_crossed_books_are_a_property_of_the_feed_not_the_decoder(replay):
    """The book is allowed to lock and cross here, and both builds must agree.

    The first version of this test asserted best bid < best ask everywhere and
    failed on instrument 84 with 19456 >= 19456. The hypothesis was a bug in
    the best-bid/best-ask walk in reduce(). It was not. market_simulator.hpp is
    a message generator, not an exchange: it drifts each instrument's reference
    price by up to 3 ticks and then places bids at ref-offset and asks at
    ref+offset, so an ask resting from before a downward drift can sit at or
    below a bid placed after it. Nothing ever matches them, because a
    normalizer's job is to reproduce the feed, not to clear it.

    So the invariant that actually holds is the one asserted here: whatever the
    feed implies, the 8-shard build must agree with the 1-shard build about it,
    down to which instruments are locked or crossed.
    """
    reference, sharded = replay
    ref_class = _classify_touch(reference)
    shard_class = _classify_touch(sharded)
    assert ref_class == shard_class
    uncrossed, locked, crossed, empty, _ = ref_class
    assert uncrossed + locked + crossed + empty == reference.num_instruments
    # Locked/crossed is the tail, not the norm; a feed where it was the norm
    # would mean the drift model had swamped the spread.
    assert locked + crossed < 0.1 * reference.num_instruments


def test_no_negative_quantities(replay):
    reference, _ = replay
    for instrument in range(reference.num_instruments):
        assert reference.bid_depth(instrument).min() >= 0
        assert reference.ask_depth(instrument).min() >= 0


# --------------------------------------------------------------------------
# Group 3: the live socket path, driven from Python
# --------------------------------------------------------------------------


def test_live_multicast_run_from_python_loses_nothing(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30017, 500_000 + 1024)
    feed.start(500_000)
    feed.wait()
    stats = feed.stats
    assert stats.sent == 500_000
    assert stats.received == stats.sent
    assert stats.loss_pct == 0.0
    assert stats.msgs_per_datagram > 10  # batching is actually happening
    assert feed.latencies_ns().shape == (stats.received,)


def test_latency_view_is_also_zero_copy(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30018, 100_000 + 1024)
    feed.start(100_000)
    feed.wait()
    a = feed.latencies_ns()
    b = feed.latencies_ns()
    assert a.__array_interface__["data"][0] == b.__array_interface__["data"][0]
    assert a.dtype == np.uint64
    assert np.percentile(a, 50) > 0


@pytest.mark.xfail(
    strict=False,
    reason=(
        "Known and deliberate: mid-run reads race the shard threads. This test asserts "
        "the thing the binding does NOT promise, so it is expected to fail. Left in the "
        "suite rather than deleted, because the limitation is the interesting part."
    ),
)
def test_midrun_snapshot_equals_final_book(mdfeed):
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30019, 600_000 + 1024)
    feed.start(600_000)
    while feed.received_so_far < 100_000:
        pass
    midrun = np.array(feed.bid_depth(0))  # forced copy, taken mid-flight
    feed.wait()
    assert np.array_equal(midrun, feed.bid_depth(0))
