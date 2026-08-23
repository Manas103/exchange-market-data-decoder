"""What the zero-copy binding costs, and what the copy it replaces would cost.

Three measurements, in the order a skeptic would ask for them:

  1. The live multicast run driven entirely from Python. This is the number on
     the resume, obtained through the binding rather than through the C++
     demo, so the binding is on the measured path and not beside it.
  2. Per-call cost of handing Python a full-depth view, against the cost of the
     same call followed by np.array(...) to force the copy the binding avoids.
     The ratio is the whole argument for nanobind here.
  3. The same comparison for the 2M-sample latency vector, where the copy is
     one large allocation rather than 1,024 small ones.

Run: python python/bench_zero_copy.py [num_messages]
"""

import pathlib
import statistics
import sys
import time

import numpy as np

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
for candidate in (REPO_ROOT / "build", REPO_ROOT / "out", REPO_ROOT):
    if candidate.is_dir() and any(candidate.glob("mdfeed_ext*")):
        sys.path.insert(0, str(candidate))
        break

import mdfeed_ext as mdfeed  # noqa: E402


def timed(fn, repeats):
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter_ns()
        fn()
        samples.append(time.perf_counter_ns() - t0)
    return statistics.median(samples)


def main():
    num_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000

    print("=" * 78)
    print("1. Live UDP multicast, publisher and decoder driven from Python")
    print("=" * 78)
    feed = mdfeed.LiveFeed(4, "239.255.10.10", 30001, num_messages + 1024)
    feed.start(num_messages)
    feed.wait()
    s = feed.stats
    print(f"Instruments with a book:   {feed.num_instruments}")
    print(f"Sent:                      {s.sent:,} messages")
    print(f"Received:                  {s.received:,} messages in {s.datagrams:,} datagrams "
          f"(avg {s.msgs_per_datagram:.1f} msgs/datagram)")
    print(f"Loss:                      {s.loss_pct:.4f}%")
    print(f"Duration:                  {s.seconds:.3f} s  "
          f"({s.msgs_per_sec:,.0f} msgs/sec observed on the wire)")

    lat = feed.latencies_ns()
    print(f"Send-to-decode latency:    p50={np.percentile(lat, 50):,.0f}ns  "
          f"p90={np.percentile(lat, 90):,.0f}ns  p99={np.percentile(lat, 99):,.0f}ns  "
          f"p99.9={np.percentile(lat, 99.9):,.0f}ns")

    resting = sum(int(feed.bid_depth(i).sum()) + int(feed.ask_depth(i).sum())
                  for i in range(feed.num_instruments))
    nonzero = sum(int((feed.bid_depth(i) != 0).sum()) + int((feed.ask_depth(i) != 0).sum())
                  for i in range(feed.num_instruments))
    print(f"Resting quantity in books: {resting:,} across {nonzero:,} occupied price levels")

    print()
    print("=" * 78)
    print("2. Cost of exposing full depth to Python: view versus copy")
    print("=" * 78)
    n = feed.num_instruments
    levels = mdfeed.LEVELS_PER_SIDE
    payload_bytes = n * 2 * levels * 8

    def sweep_views():
        for i in range(n):
            feed.bid_depth(i)
            feed.ask_depth(i)

    def sweep_copies():
        for i in range(n):
            np.array(feed.bid_depth(i))
            np.array(feed.ask_depth(i))

    view_ns = timed(sweep_views, 21)
    copy_ns = timed(sweep_copies, 21)
    calls = n * 2
    print(f"Books:                     {n} instruments x 2 sides x {levels:,} levels "
          f"= {payload_bytes / 1e6:.1f} MB of level data")
    print(f"Zero-copy sweep:           {view_ns / 1000:,.1f} us total, "
          f"{view_ns / calls:,.0f} ns per view")
    print(f"Materialized sweep:        {copy_ns / 1000:,.1f} us total, "
          f"{copy_ns / calls:,.0f} ns per copy")
    print(f"Ratio:                     {copy_ns / view_ns:.1f}x  "
          f"(bytes copied: 0 versus {payload_bytes / 1e6:.1f} MB)")

    print()
    print("=" * 78)
    print("3. Same comparison for the latency vector (one big buffer)")
    print("=" * 78)
    lat_bytes = lat.size * 8
    lat_view_ns = timed(feed.latencies_ns, 21)
    lat_copy_ns = timed(lambda: np.array(feed.latencies_ns()), 21)
    print(f"Samples:                   {lat.size:,} uint64 ({lat_bytes / 1e6:.1f} MB)")
    print(f"Zero-copy handoff:         {lat_view_ns:,.0f} ns")
    print(f"Materialized copy:         {lat_copy_ns:,.0f} ns")
    print(f"Ratio:                     {lat_copy_ns / max(lat_view_ns, 1):,.0f}x")

    print()
    print("Note: the ratios above are the cost of the handoff, not of the analysis that")
    print("follows it. Reducing over a view and reducing over a copy cost the same once")
    print("the copy exists. What the binding removes is the allocation and the memcpy on")
    print("every single access, which is what makes polling a live book from Python")
    print("viable at all.")


if __name__ == "__main__":
    main()
