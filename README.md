# Exchange Market Data Decoder, Order Book Normalizer, and Zero-Copy Python Interface

A C++17 decoder for a simulated exchange's binary multicast feed, modeled on the
core ideas behind Nasdaq-style ITCH protocols: a compact, fixed-width message
stream describing order-book events (add, execute, cancel, delete, replace),
decoded into full-depth limit order books for 512 instruments by a sharded,
multithreaded normalizer, with a nanobind extension that hands Python live
full-depth views of those books without copying a byte. Extended with a
queue-position backtest simulator and, most recently, a cross-venue NBBO
consolidator and trade-through guard modeling Regulation NMS Rules 611 and
610(e) over three simulated venues. Every number in this README was measured
on the machine described below, not targeted. The one
number that is not a throughput figure is the one worth reading first: the
NumPy buffer address returned by `bid_depth(i)` is asserted equal to the
address of the C++ level array, which is the only evidence that "zero copy"
means anything.

## Why this exists

Exchange feed handlers are one of the more interesting boring-infrastructure
problems in trading systems: the format is simple, but doing it fast and
correctly at high message rates, across hundreds of instruments, with multiple
threads touching the book, is genuinely hard to get right. And the moment the
book is correct, somebody wants to look at it from Python, which is where most
research code lives. That second half is the part usually solved badly, by
marshalling a dict of levels across the boundary on every access.

This project builds the whole pipeline (protocol, decoder, order book,
multithreaded normalizer, Python interface), proves it is correct against a
reference build, and measures how fast it actually is, rather than asserting a
number.

## Honest framing, up front

- **The protocol is ITCH-inspired, not ITCH 5.0.** Real Nasdaq TotalView-ITCH
  has a much larger message set and an exact byte-level field layout published
  by Nasdaq. This project implements the same category of messages
  (Add, Execute, Cancel, Delete, Replace) with a simplified, self-documented
  binary framing (`include/protocol.hpp`), enough to be a legitimate decoder
  engineering exercise without claiming spec conformance it does not have.
- **The feed is synthetic.** `include/market_simulator.hpp` generates a
  self-consistent random order flow (every Execute, Cancel, Delete and Replace
  references a real, currently resting order) across 512 instruments with
  slowly drifting reference prices. There is no real exchange data behind this.
- **This is a normalizer, not an exchange.** Nothing here matches orders. The
  books lock and cross on a small number of instruments and that is correct
  behavior for this feed, not a bug; see Findings.
- **Windows and Winsock only.** The live multicast path was built and tested
  with MSVC and Winsock2 in this environment. A portable version would abstract
  the socket layer behind an interface with a Linux backend, but that was not
  available to test here, so it is kept explicit rather than pretending
  portability that was never exercised. No sanitizer runs are committed for
  this repo for the same reason: the network path does not build on Linux,
  where ASan and TSan live.
- **Zero copy means no copy on read, not no copy anywhere.** Messages are
  memcpy'd off the wire into the decoder. What the binding removes is the copy
  between the C++ book and Python, which is the copy that would otherwise
  happen on every single access.
- **Reads taken while the feed is running race the shard threads.** They are
  not synchronized. On x86_64 an aligned 8-byte load does not tear, so a
  mid-run read returns a real level quantity from some recent point in time,
  but it is a data race by the letter of the C++ memory model. Every verified
  number below is read after `wait()`, when the shards are joined and the books
  are quiescent. The test suite contains an `xfail` test that asserts the
  opposite, deliberately, so the limitation is executable rather than a
  sentence in a README.

### Machine and toolchain

| | |
|---|---|
| CPU | AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores |
| RAM | 31.1 GB |
| OS | Windows 11, build 10.0.26200 |
| Compiler | MSVC 19.29.30159 (Visual Studio 2019 Build Tools), `/O2 /W4 /EHsc`, C++17 |
| Build | CMake 4.2.0, NMake Makefiles, `CMAKE_BUILD_TYPE=Release` |
| Python | CPython 3.12.10 (64-bit), nanobind 3.0.0, NumPy 2.3.2, pytest 9.1.0 |
| Network | loopback multicast, group 239.255.10.10, `SO_RCVBUF` 8 MB |
| Pacing | non-realtime process, no CPU pinning, no `SCHED_FIFO` equivalent, caches warm after the first run |

## Architecture

```
include/protocol.hpp          simplified ITCH-style binary message definitions
include/market_simulator.hpp  synthetic, self-consistent order-flow generator
include/order_book.hpp        per-instrument limit order book (flat price-level array)
include/ring_buffer.hpp       lock-free SPSC ring buffer (atomics, no locks or CAS loops)
include/normalizer.hpp        sharded multithreaded normalizer (instrument_id % N -> shard)
include/multicast_udp.hpp     Winsock UDP multicast sender and receiver
include/live_feed.hpp         publisher + receiver + normalizer as one object; offline replay
include/latency_stats.hpp     percentile reporting helper

src/decoder_bench.cpp         offline throughput/latency benchmark (bypasses the network)
src/live_multicast_demo.cpp   real UDP multicast publisher + live decoder, batched
tests/correctness_test.cpp    diffs a single-threaded reference book against the sharded one

python/mdfeed_ext.cpp         nanobind module: zero-copy NumPy views of live books
python/bench_zero_copy.py     live run from Python, then view-versus-copy timing
python/tests/conftest.py      finds the built .pyd without an install step
python/tests/test_zero_copy.py  pointer identity, lifetime, and a NumPy reference diff

docs/test_output.txt          raw C++ correctness run
docs/benchmark_output.txt     raw live multicast and offline benchmark runs
docs/python_test_output.txt   raw pytest run
docs/python_benchmark_output.txt  raw zero-copy benchmark run
```

**Order book.** Aggregated price levels live in a flat array indexed by tick
offset from a per-instrument base price, not a `std::map<price, qty>`. Real
books stay within a bounded band around the last trade (exchanges halt or widen
limits long before price moves thousands of ticks), so a fixed-size array gives
O(1) level updates instead of O(log n) tree operations. The tradeoff, stated
plainly: a price outside the pre-sized band is a bug, not a graceful path, so
the level index is bounds-checked and out-of-range updates are dropped with a
counter rather than corrupting memory or throwing mid-feed.

That layout choice is also what makes the Python side cheap. A `std::map` book
has nothing to hand NumPy; a flat `int64_t[16384]` is already a NumPy array,
and the binding is a pointer and a shape. The design decision that was made for
cache behavior in the C++ hot path is the same decision that removes the
marshalling cost at the language boundary, which is not a coincidence so much
as the reason flat layouts keep winning.

**Sharding.** Every message for a given instrument routes to
`instrument_id % num_shards`, so each shard thread owns its books and its
`order_id -> location` map with zero synchronization on book state. The only
synchronization anywhere in the hot path is a lock-free SPSC ring buffer per
shard, fed by one dispatcher thread. A single-producer/single-consumer queue is
enough because sharding guarantees exactly one producer and one consumer per
queue; it is not a general MPMC structure.

**One implementation of the live path.** `include/live_feed.hpp` owns the
publisher thread, the receiver thread, and the normalizer, and both entry
points use it: the C++ demo and the nanobind module. This matters more than it
looks. The binding's headline claim is that Python reads the same book memory
the decoder writes. If the binding had its own receive loop, that would be a
claim about two similar programs rather than about one program.

**The binding.** `python/mdfeed_ext.cpp` returns `nb::ndarray<nb::numpy, const
int64_t>` objects constructed directly over `OrderBook::bid_data()`. Three
properties make that safe rather than reckless:

- The owning Python object is passed as the ndarray's `owner`, so nanobind
  keeps the feed alive as long as any view survives. Dropping the feed while
  holding an array does not dangle, and there is a test that drops it.
- The scalar type is `const`, so NumPy marks the array non-writeable. Python
  can read a live book; it cannot corrupt one the decoder threads own.
- `start()` and `wait()` are bound with `nb::call_guard<nb::gil_scoped_release>`,
  so the C++ threads run while Python is free to poll progress and hold views.

## Validation

Four layers, all with raw output committed under `docs/`.

**1. Reference build diff, in C++.** `tests/correctness_test.cpp` replays an
identical 2,000,000-message stream through a 1-shard (effectively
single-threaded) `Normalizer` and an 8-shard one, then diffs every instrument's
best bid and ask plus 601 individual price levels per side around the reference
price:

```
Checked 307712 price levels across 512 instruments.
PASS: multithreaded normalizer output is bit-for-bit identical to the
single-threaded reference.
```

**2. The same diff, in NumPy, over zero-copy views.** The Python suite builds
the same pair of reference and sharded books and diffs them through the
binding, which puts the binding on the correctness path rather than beside it.
It runs the exact 307,712-level window the C++ test uses and then a wider one:
every level of every book, both sides, 16,777,216 levels, exact.

**3. Cross-language oracle on top of book.** C++ maintains best bid and best
ask as running indices updated on every event. NumPy recomputes them from
scratch with `np.nonzero(...).max()` off the raw level array. Two different
algorithms over the same memory, agreeing across all 512 instruments, is a real
check on the incremental bookkeeping rather than a tautology.

**4. Pointer identity and lifetime.** No benchmark can prove the absence of a
copy, because a fast copy is still a copy. The only proof is
`array.__array_interface__["data"][0] == feed.depth_ptr(i)`, asserted for both
sides of four instruments, plus tests that repeated calls return the same
buffer, that the array is non-writeable, that the view outlives `del feed`, and
that a view fetched before a run reflects the book after it without being
refetched.

```
13 passed, 1 xfailed in 7.07s
```

The `xfail` is `test_midrun_snapshot_equals_final_book`. It asserts that a
snapshot taken while the feed is running equals the final book, which is
exactly the guarantee this binding does not make. It is left in the suite
rather than deleted, because the limitation is the interesting part.

## Findings

**The book crosses, and that is the feed's fault, not the decoder's.** The
first version of the Python invariant suite asserted best bid strictly below
best ask on every instrument. It failed on instrument 84 with `19456 >= 19456`.
The first hypothesis was a bug in the best-bid walk in `OrderBook::reduce()`,
which does a linear scan backwards when the top level empties, and which is the
one piece of index bookkeeping in the class that is easy to get wrong.

The measurement that discriminated: classify all 512 instruments rather than
failing on the first, and run the classification against both the 1-shard and
8-shard builds. If the walk were buggy, the two builds would disagree about
which instruments were crossed, because they process events in different
interleavings. They agreed exactly: 494 uncrossed, 13 locked, 5 crossed, in
both builds.

Root cause: `market_simulator.hpp` drifts each instrument's reference price by
up to 3 ticks with probability 0.0005 per message, then places bids at
`ref - offset` and asks at `ref + offset`. An ask resting from before a
downward drift can end up at or below a bid placed after it, and nothing ever
removes the pair, because a normalizer reproduces the feed rather than clearing
it. The fix was to the test, not the code: the invariant that actually holds is
that both builds agree about the crossed set, and that is what is asserted now.

The method mattered more than the answer. "Diff two builds" turned a suspected
concurrency bug into a five-minute property of the generator, and it is the
same instrument as the 307,712-level check, applied to a different question.

**Constructing a feed and never running it aborted the process.** The first
version of `LiveFeed` started the normalizer's shard threads in the constructor
and only joined them in `wait()`. Three of the pointer-identity tests build a
feed, read an address, and drop it without publishing anything, so the
`std::thread` members were still joinable at destruction and the runtime called
`std::terminate`. It surfaced as `Fatal Python error: Aborted` with a pytest
traceback and no test name, which is the least informative failure mode
available. The destructor now drains unconditionally. Worth noting because the
C++ demo never hit it: the demo always publishes, so the only caller that could
find this bug was the binding.

**Batching, not decode cost, is what the network path measures.** An early
version of the publisher sent one message per `sendto()` and was syscall-bound
at roughly 13K msgs/sec. Real exchange feeds pack several messages per
datagram, and once this one does too (up to about 1,200 payload bytes, 41.3
messages per datagram measured), throughput moves by more than an order of
magnitude. The gap between the live figure and the offline figure below is the
honest point of the whole project.

## Measured results

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, 31.1 GB RAM,
Windows 11 build 10.0.26200, MSVC 19.29.30159 with `/O2`, CPython 3.12.10 with
nanobind 3.0.0 and NumPy 2.3.2. Non-realtime process, no CPU pinning. Raw
output in `docs/`.

Throughput on the live socket varies run to run by roughly 10% on this machine
(three consecutive 2M-message runs gave 331,847, 343,322 and 374,199 msgs/sec),
so ranges are given where that matters.

### Live UDP multicast, driven from Python (`python/bench_zero_copy.py`)

This is the binding on the measured path: Python constructs the feed, the
publisher and receiver threads run with the GIL released, and Python reads the
result out of the books through zero-copy views.

| Metric | Measured |
|---|---|
| Instruments with a full-depth book | 512 |
| Messages sent | 2,000,000 |
| Messages received and decoded | 2,000,000 in 48,438 datagrams (41.3 msgs/datagram) |
| **Loss** | **0.0000%** |
| **Wire throughput** | **343,322 msgs/sec** (range across runs: 331,847 to 374,199) |
| Send-to-decode latency | p50 74.7us, p90 91.5us, p99 159.1us, p99.9 225.3us |
| Resting quantity in books at end | 184,470,496 across 203,698 occupied price levels |

What "send-to-decode latency" measures: the time from the publisher stamping a
message to the receiver having decoded it, including the full OS network stack
on loopback. It does not measure the shard thread's book update, which is
counted separately below, and it is not comparable to a NIC-to-application
figure on real hardware.

### Zero-copy handoff cost versus the copy it replaces

| Handoff | Payload | Per call | Total sweep | Bytes copied |
|---|---|---|---|---|
| `bid_depth`/`ask_depth`, 1,024 calls | 134.2 MB of level data | **306 ns** | 313.3 us | 0 |
| Same, each followed by `np.array(...)` | 134.2 MB | 5,443 ns | 5,573.9 us | 134.2 MB |
| `latencies_ns()`, one call | 16.0 MB (2M uint64) | **300 ns** | | 0 |
| Same, followed by `np.array(...)` | 16.0 MB | 1,811,000 ns | | 16.0 MB |

**17.8x** on the per-book sweep and **6,037x** on the single large buffer. The
two ratios differ for the obvious reason: the per-book case is dominated by
1,024 Python call overheads that both variants pay, while the latency-vector
case is a single call where the copy is nearly all of the cost. The honest
reading is that the ratio is a property of payload size per call, not a
property of nanobind, and a binding that returned one level at a time would
show no advantage at all.

Where this approach loses: if the consumer is going to mutate the data or hold
it past the feed's lifetime, it needs the copy anyway, and the view has bought
nothing but an extra indirection and a lifetime hazard. The binding is worth it
for read-mostly polling of live state, which is what a research process
attached to a feed handler actually does.

### Offline decode and book-update throughput (`decoder_bench.exe`)

This isolates decode and order-book-maintenance cost by pre-generating all
messages in memory before timing starts. It deliberately excludes network I/O.
Run on 20,000,000 messages at 8 shards:

| Metric | Measured |
|---|---|
| **Throughput** | **17,008,323 msgs/sec** |
| Per-message processing latency | p50 200 ns, p90 500 ns, p99 900 ns, p99.9 1,500 ns |

Scaling is close to linear from 1 to 8 shards (matching the 8 physical cores)
and flattens past that, as expected once hyperthreading and dispatcher-thread
contention take over. There is an occasional multi-millisecond outlier in the
max, which is OS thread scheduling preemption on a non-realtime, non-isolated
process; it is visible in the raw output rather than hidden.

The `e2e_latency` line in the raw output is queue residency under a dispatcher
that enqueues far faster than eight shards can drain, so it grows to hundreds
of milliseconds and measures backpressure, not decode. It is reported rather
than dropped, but it is not a latency figure anyone should quote.

The gap between 17.0M msgs/sec offline and 343K msgs/sec on the wire is the
honest point: decode and book maintenance are not the bottleneck in a naive UDP
publisher, socket syscalls and batching policy are. Real feed handlers solve
this with larger batches, multiple sockets, and often kernel-bypass NICs, which
is out of scope for a portfolio project's I/O layer but worth naming rather
than glossing over.

### Correctness

| Check | Result |
|---|---|
| C++ reference diff, 2M messages, ref +/- 300 ticks | **307,712 price levels across 512 instruments, 0 mismatches** |
| NumPy diff through the binding, same window | 307,712 levels, 0 mismatches |
| NumPy diff, full depth both sides | 16,777,216 levels, 0 mismatches |
| Top of book recomputed in NumPy vs C++ incremental | 512 instruments, exact |
| Touch classification, 1-shard vs 8-shard | identical (494 uncrossed, 13 locked, 5 crossed) |
| pytest | 13 passed, 1 xfailed |

## Building and running

Requires CMake and a C++17 compiler. Tested with MSVC (Visual Studio 2019 Build
Tools) on Windows. Winsock (`multicast_udp.hpp`) is only compiled on Windows;
the protocol, book, ring buffer and normalizer are portable standard C++.

```bat
:: from a Developer Command Prompt for VS 2019, or after running vcvars64.bat
mkdir build && cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release ..
nmake

correctness_test.exe 2000000
decoder_bench.exe 20000000 8
live_multicast_demo.exe 2000000
```

For the Python extension, add `-DBUILD_PYTHON_BINDING=ON`. nanobind is located
through whichever interpreter is used, so there is no vendored copy and no
submodule:

```bat
pip install nanobind numpy pytest

mkdir build && cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release -DBUILD_PYTHON_BINDING=ON ^
      -DPython_EXECUTABLE=C:/Path/To/python.exe ..
nmake
cd ..

python -m pytest python/tests -v
python python/bench_zero_copy.py 2000000
```

The test suite finds the built `.pyd` in `build/` itself (see
`python/tests/conftest.py`), so no install step is needed. If the extension is
absent the suite skips rather than fails.

Using the binding directly:

```python
import mdfeed_ext

feed = mdfeed_ext.LiveFeed(num_shards=4, capacity=2_000_001)
feed.start(2_000_000)
while feed.running and feed.received_so_far < 1_000_000:
    pass
depth = feed.bid_depth(42)          # NumPy view, no copy, updates as the feed runs
feed.wait()                          # after this, reads are exact
print(feed.stats, depth.sum())
```

## Sibling comparison

[`market-data-tick-capture`](https://github.com/Manas103/market-data-tick-capture)
solves the adjacent problem: it decodes the same category of feed but persists
it in a column-oriented capture format with nanosecond timestamps, and it also
exposes a zero-copy Python reader. The contrast is worth stating because the
two repos make the opposite call about where the Python boundary sits.

| | this repo | market-data-tick-capture |
|---|---|---|
| What Python sees | live in-memory books, mutating under the reader | an immutable capture file, memory-mapped |
| Binding mechanism | nanobind ndarray over `int64_t[16384]` | buffer protocol over mapped columns |
| Zero-copy is safe because | the feed object owns the buffer and outlives the view | the data never changes after the file is written |
| Headline number | 343,322 msgs/sec on the wire, 0% loss, 512 books | 4.71x smaller than row format, 815,678 msgs/sec drain |

Reading a live book is the harder side of that table, and the reason is the row
about safety: the capture reader gets its guarantee for free from immutability,
while this repo has to buy it with a lifetime contract and an explicit
statement about mid-run races. That is the design decision this repo is
defending, and it is why the `xfail` test exists.

## Extended (Sep. 2026): Queue-Position Backtest Simulator

The book-building core above is reused unchanged. Everything in this section
is new: a queue-position tracking layer, a naive comparison model, and a
backtest CLI that replays the decoder's own synthetic message stream and
reports fill rates and a strategy Sharpe under both models.

**What is new versus what is reused.** `OrderBook` and `MarketSimulator` are
the exact classes described above, driving a single-threaded, single-
instrument replay (a backtest is an offline batch job; the sharding and
multithreading exist to hide network and syscall latency during live
ingestion, which does not apply here). `include/queue_book.hpp` adds a FIFO
queue-position tracker per price level, keyed on the sequence number at which
a resting order joined the level, aged forward as ahead orders are canceled
or executed. `include/backtest_sim.hpp` places a naive/queue-tracked pair of
orders at the current best bid on a fixed cadence and resolves each against
the replay until it fills, is canceled at a TTL, or the replay ends.

**Honest framing, up front.**
- **This is a backtest simulator, not a live trading system.** No orders are
  ever sent anywhere; everything replays a pre-generated synthetic message
  stream.
- **The naive model is deliberately unrealistic.** It marks a resting order
  filled the instant any execution touches its price level, regardless of
  whether that specific order was next in line. That is the textbook mistake
  this project exists to quantify, not a strawman.
- **The book this replays is the one described above, crossed feed and all.**
  The base repo's Findings section already documents that this OrderBook can
  be crossed or locked because the synthetic feed has no matching logic. The
  backtest inherits that property, and a fill observed while the book is
  crossed has no economically meaningful spread to capture, so the P&L
  analysis below reports and excludes those fills rather than pretending they
  are normal quotes; see `python/analyze_backtest.py`'s own note.
- **The fee and cost model is illustrative, not any specific venue's
  schedule.** Net P&L subtracts a flat $0.0005/share fee on both legs of a
  round trip and gives back half the captured spread as the realistic cost of
  exiting the position; see the docstring in `python/analyze_backtest.py`.

### Architecture (additions)

```
include/queue_book.hpp          FIFO queue-position tracker per price level, ages through cancels/executes ahead
include/backtest_sim.hpp        BacktestEngine: replays the message stream, runs naive vs queue-tracked fill models
src/backtest_cli.cpp            CLI: replay N messages, write a flat CSV of every order's outcome
tests/queue_position_reference_test.cpp  hand-computed scenario + a from-scratch oracle diffed against the fast tracker
python/analyze_backtest.py      turns the CSV into fill rates and gross/net Sharpe
docs/queue_position_test_output.txt   raw reference-test run
docs/backtest_benchmark_output.txt    raw backtest_cli run
docs/backtest_analysis_output.txt     raw analyze_backtest.py run
docs/backtest_results_sample.csv      first 100 rows of a run's output, for inspection
```

**Why a CSV, not a nanobind binding.** `python/mdfeed_ext.cpp`'s value is a
zero-copy live view over a book that mutates while Python watches it. This
backtest is an offline, one-shot batch replay producing a single flat table of
finished orders; Python's only remaining job is arithmetic over that table. A
CSV artifact is more honest about what this is, easier to test end to end
without a compiled extension, and easier to commit a sample of under `docs/`.

**Queue-position aging.** `QueuePositionBook` assigns every resting order a
monotonically increasing sequence number when it joins a level, and tracks
the total quantity ahead of a given sequence at that level. A `FastAheadTracker`
starts with the ahead quantity measured at placement and decrements it as
`ApplyEffect` reports executions or cancels at that level with a lower
sequence number; it never decrements on activity behind the tracked order,
and an order behind never advances one ahead of it. This is the entire
contract the reference test in the next section checks.

### Validation

**1. Hand-computed scenario.** A 6-step sequence (add ahead, add ours, cancel
part of what's ahead, execute part of what's ahead, add behind, execute what's
left ahead) is worked out by hand and diffed exactly against the tracker's
output at every step.

**2. From-scratch oracle over a long replay.** A second, deliberately slow
tracker recomputes ahead quantity by rescanning every order at the level from
scratch after every message, rather than incrementally. Diffed against the
fast incremental tracker over an 80,000-message replay: 491,087 ahead-quantity
comparisons, 0 mismatches.

```
PASS: hand-computed scenario, 6 steps, all exact.
Long replay: 80000 messages, 491087 ahead-qty comparisons, 0 fills observed, 0 mismatches.
PASS: fast incremental tracker matches the from-scratch oracle exactly.
```

**3. Structural invariant.** Naive fill rate is always >= queue-tracked fill
rate on identical placement decisions, because naive assumes the best case
(any execution at the level fills you) and queue-tracking only ever adds a
stricter condition on top of it. This held in every run in this section.

### Findings

**The fill-rate and Sharpe targets were not reached, after three genuine
attempts, and the reason is structural, not a tuning miss.** The first attempt
(2,000,000 messages, 5,000-message TTL) measured a 1.53% naive fill rate. The
hypothesis was that the TTL was too short for an order to wait its turn, so
the second attempt raised the TTL to 50,000 messages over 3,000,000 messages;
fill rates rose only to 3.67% naive and the naive-to-queue ratio widened to
about 46x, in the wrong direction for matching the 91%/38% target ratio of
about 2.4x. The measurement that discriminated: `market_simulator.hpp` adds
new orders faster than it removes them (55% add vs. 45% remove/replace on any
given non-empty-book message), so the book at a given price level grows
without bound over the length of a replay, and `MarketSimulator` picks the
order to execute or cancel uniformly at random from the *entire* book, not
weighted toward the touch. Both effects mean the specific price level a
resting order sits at, and the specific queue position within it, receive a
shrinking share of the replay's activity the longer the replay runs and the
larger the book gets. The third attempt (300,000 messages, 200-message
placement cadence, 20,000-message TTL) placed orders earlier in the replay,
while the book was still small, and measured the best fill rates of the three
attempts: 26.68% naive, 0.20% queue-tracked. That is the number reported
below. Root cause, stated plainly: this repo's synthetic feed was built to
exercise a decoder and normalizer, not to reproduce the touch-concentrated
order flow of a real limit order book, where the overwhelming majority of
volume and cancellations cluster within a few ticks of the touch. A feed with
that property would need a different generator (see Limitations), which is
out of scope for this extension; the honest result is reported instead of
tuned to match the target.

**The crossed-book property, already documented above, dominates the P&L
sample.** 94.2% of naive fills occurred while the book was crossed, because a
crossed or locked touch is exactly where two arbitrarily-priced resting orders
are most likely to have their price levels coincide. `analyze_backtest.py`
excludes crossed-book fills from the Sharpe calculation rather than reporting
a P&L number computed against a negative or zero "spread", which would not be
economically meaningful. This leaves 23 valid naive fills and only 2 valid
queue-tracked fills to compute a Sharpe from, small samples disclosed as such
rather than hidden.

### Measured results

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, 31.1 GB RAM,
WSL2 Ubuntu 22.04 on Windows 11 build 10.0.26200, g++ 11.4.0 with `-O3`,
CMake 4.2.0, Python 3.12. Raw output in `docs/`. Configuration: 300,000
messages, seed 42, a new order pair placed every 200 messages, 20,000-message
time-to-live, 100-share order size (the best of three genuine attempts; see
Findings).

| Claim | Target | Measured | Meets claim |
|---|---|---|---|
| Naive fill rate | 91% | **26.68%** (400/1,499 orders) | No |
| Queue-tracked fill rate | 38% | **0.20%** (3/1,499 orders) | No |
| Gross Sharpe | 2.4 | **1.82** (naive, n=23 valid fills) | No |
| Net Sharpe (after fees and half-spread) | 0.6 | **1.64** (naive, n=23 valid fills) | Measured higher, not lower |

Queue-tracked gross/net Sharpe is not reported as a number: only 2 of its 3
fills occurred on a non-crossed book, both with an identical measured spread,
so the sample's standard deviation is exactly 0 and Sharpe is undefined for
this run. Its raw sum was $4.00 gross / $1.80 net over those 2 fills.

What each number does and does not measure: fill rate is exact (every placed
order resolves to filled, canceled, or still resting; the CSV in `docs/`
records all of them). Sharpe here is a per-fill statistic (mean divided by
population standard deviation of per-fill P&L), not an annualized daily
Sharpe; there is no calendar-time axis in an event-driven replay of this
length, and stating that plainly is more honest than dividing by an arbitrary
`sqrt(252)`.

### Building and running

```bash
# WSL2 Ubuntu 22.04, g++ 11.4
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(( $(nproc) / 2 ))" backtest_cli queue_position_reference_test

./queue_position_reference_test
./backtest_cli 300000 /tmp/backtest_results.csv 42 200 20000 100

cd ..
python3 python/analyze_backtest.py /tmp/backtest_results.csv
```

`backtest_cli` also builds under the MSVC/NMake path described above (it has
no Winsock dependency), but the runs reported here were made under WSL2 for
a faster edit-build-measure loop.

## Extended (Sep. 2026): Cross-Venue NBBO Consolidator & Trade-Through Guard

**Not real Regulation NMS.** This is the market-structure arithmetic Rule 611
(trade-through) and Rule 610(e) (locked and crossed markets) are about,
implemented against three simulated venues, not the real rule text and not a
connection to any real exchange.

**What existed before this extension and what is new.** The base decoder and
normalizer already give a single venue's full-depth book. Nothing in the
repo, before this, ever looked at more than one venue at once. This
extension adds exactly that layer: three independent simulated venues each
quoting the same synthetic symbol, a consolidator that computes the National
Best Bid and Offer across them, a locked/crossed classifier, and an outbound
order guard that refuses to send an order through a better protected price
sitting at another venue.

### Architecture (additions)

```
include/venue_feed.hpp          One simulated venue: MarketSimulator's order
                                 flow replayed into one OrderBook, with the
                                 order_id -> (side, price, qty) bookkeeping a
                                 standalone book needs for Cancel/Execute/
                                 Delete/Replace (normalizer.hpp carries the
                                 same bookkeeping per-shard; this is the
                                 single-book version).
include/nbbo.hpp                NbboAggregator::best_bid/best_offer (the
                                 consolidated NBBO across N venues) and
                                 ::classify (every distinct-venue bid/offer
                                 pair that is locked or crossed), plus an
                                 O(kLevels) from-scratch oracle for both.
include/trade_through_guard.hpp Given a candidate outbound order and the
                                 current NBBO, decides accept/reject: a buy
                                 above the NBBO offer or a sell below the
                                 NBBO bid is a trade-through and is rejected.
src/nbbo_bench.cpp               Benchmark: NBBO republish latency, one venue
                                 update at a time.
src/nbbo_fault_injection.cpp     2,000 seeded trade-through scenarios plus 40
                                 clean sessions, counting catches and false
                                 positives.
tests/nbbo_reference_test.cpp    NBBO and locked/crossed diff against an
                                 independent from-scratch oracle over a long
                                 random replay, plus two hand-built explicit
                                 locked and crossed cases (see Findings).
```

The trade-through guard is deliberately a pure function of (candidate order,
current NBBO), not a stateful component: `guard(order, nbbo) -> accept |
reject(reason)`. That keeps it trivially unit-testable and keeps the
question "would this specific order have traded through" answerable without
replaying anything.

### Validation

Two independent layers, run under both a Release build and an ASan+UBSan
Debug build (`docs/nbbo_reference_test_san.txt`,
`docs/nbbo_fault_injection_san.txt`, `docs/nbbo_bench_san.txt`):

1. **Oracle diff.** `NbboAggregator::best_bid`/`best_offer` read each venue's
   incrementally maintained best-bid/best-ask index. `oracle_best_bid`/
   `oracle_best_ask` instead scan all 16,384 price levels of that venue's
   book from scratch. `classify()` is diffed the same way, against a
   from-scratch pairwise classification built directly from the oracle
   quotes. 7,200 NBBO comparisons and 3,600 locked/crossed comparisons over
   a 90,000-message-per-venue replay, 0 mismatches
   (`docs/nbbo_reference_test_output.txt`).
2. **Explicit positive locked/crossed cases.** The random replay above
   essentially never produces a naturally locked or crossed market (three
   independent random walks around the same reference price rarely invert;
   see the `locked pairs seen=0, crossed pairs seen=0` line in the same
   output). An oracle diff over data that never contains the case it is
   supposed to check proves nothing about that case, so two hand-built
   books are also checked directly against `classify()`: a bid at 250.10 on
   venue 0 against an offer at 250.10 on venue 1 (locked), and a bid at
   250.20 on venue 0 against an offer at 250.05 on venue 1 (crossed). Both
   are correctly named.
3. **Seeded fault injection.** `nbbo_fault_injection` constructs exactly
   2,000 orders engineered to trade through the NBBO and 40 sessions of
   1,500 total clean orders that never should. Every seeded trade-through is
   caught; zero clean orders are rejected (`docs/nbbo_fault_injection_output.txt`).

### Findings

**A packed-struct alignment bug UBSan caught on its first run of this code,
before the sanitizer had ever been run on this repository at all.**
`WireMsg`'s variants are `#pragma pack(1)` (`protocol.hpp`), which is
correct for a wire format: no compiler-inserted padding between fields.
`VenueFeed::apply` originally read fields like `m.add.order_id` straight
through as the argument to `unordered_map::operator[]`/`::find`, and
`m.exec.exec_qty` straight into `std::min`. Both bind a `const T&` parameter
to the packed field. That is undefined behavior: the reference's declared
type (`const uint64_t&`, `const uint32_t&`) carries no record that the
object underneath is only byte-aligned, so the compiler is entitled to
assume natural alignment for anything accessed through it, and UBSan checks
exactly that assumption. The first sanitizer run of `nbbo_reference_test`
failed immediately with "reference binding to misaligned address ...
which requires 8 byte alignment" inside `VenueFeed::apply`, and a second
pass (after fixing the `order_id` sites) turned up the identical class of
bug one level down, inside `std::min`'s own reference parameters for
`exec_qty`/`cancel_qty`. The fix in both cases is the same: copy the packed
field into a local, naturally-aligned variable before it is used anywhere a
reference could be bound to it, which is what `venue_feed.hpp` now does at
every such call site. This never crashed on x86-64, which tolerates
unaligned scalar loads at the hardware level; it is exactly the kind of bug
that is silent here and not silent on a stricter target, and the reason to
run the sanitizer is to find it before that target does. The same
read-a-packed-field-into-an-unordered_map-key pattern already exists in the
base repo's `normalizer.hpp` for the identical reason; it is not touched
here because it is out of scope for this extension, but it is the same bug
and would fail the same way under a sanitizer.

**Locked and crossed markets are structurally rare in this synthetic
setup.** With three venues doing independent random walks around one shared
reference price, a bid at one venue overtaking an offer at another is a
low-probability event; across roughly 2.4 million total book updates in the
reference test and benchmark runs combined, exactly zero occurred. The guard
and classifier are proven correct against explicit constructed cases (above)
rather than relying on chance occurrence in random synthetic data, and that
is disclosed rather than left implicit.

### Measured results

Machine: AMD Ryzen 7 7800X3D, WSL2 Ubuntu 22.04 (12 logical cores visible to
WSL under its `.wslconfig` cap; 8 physical / 16 logical on the host), Windows
11 build 10.0.26200, g++ 11.4.0 with `-O3`, CMake 3.22.1. Raw output in
`docs/`.

| Claim | Target | Measured | Meets claim |
|---|---|---|---|
| Cross-venue NBBO consolidation | 3 simulated venues | **3** venues, 7,200 NBBO comparisons vs. oracle, 0 mismatches | Yes |
| NBBO republish latency | p99 < 2us from a venue update | **p99 60ns** (mean 20ns, max 46,412ns) over 2.1M venue updates | Yes |
| Locked and crossed quote detection | implemented and correct | 3,600 oracle comparisons + 2 explicit hand-built cases, 0 mismatches; 0 naturally-occurring cases in the synthetic replay (see Findings) | Yes (disclosed: not exercised by chance in random data) |
| Trade-through guard | rejects orders that would trade through | Implemented as a pure function of (order, NBBO); see fault injection below | Yes |
| Seeded trade-throughs caught | 2,000 / 2,000 | **2,000 / 2,000** | Yes |
| False positives over clean sessions | 0 over 40 sessions | **0** over 40 sessions, 1,500 clean orders | Yes |

The republish-latency benchmark measures the cost of one call to
`best_bid`/`best_offer`/`classify` after a single venue's book changes; it
does not include the cost of decoding the wire message that caused the
change (that is `decoder_bench`'s number, reported above) or any network
hop. `max=46,412ns` on one update out of 2.1 million is consistent with an
OS scheduling preemption on a shared machine, not a property of the
algorithm; p99.9 is 110ns.

### Building and running

```bash
# WSL2 Ubuntu 22.04, g++ 11.4
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j"$(( $(nproc) / 2 ))" nbbo_reference_test nbbo_fault_injection nbbo_bench

./nbbo_reference_test        # oracle diff + explicit locked/crossed cases
./nbbo_fault_injection       # 2,000 seeded trade-throughs + 40 clean sessions
./nbbo_bench                 # republish latency

cd ..
```

ASan+UBSan build (the one that found the alignment bug above):

```bash
mkdir -p ~/build/emdd-san && cd ~/build/emdd-san
cmake /path/to/exchange-market-data-decoder \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g -O1" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
make -j"$(( $(nproc) / 2 ))" nbbo_reference_test nbbo_fault_injection nbbo_bench
./nbbo_reference_test && ./nbbo_fault_injection && ./nbbo_bench 50000
```

These three targets are portable standard C++ with no Winsock dependency, so
unlike `live_multicast_demo` they build and run under WSL2 directly.

## Layout

See the Architecture section.

## Limitations

- Simplified ITCH-style protocol, not the real Nasdaq ITCH 5.0 spec.
- Synthetic order flow, not a captured real exchange feed.
- Windows and Winsock only network layer for the original decoder and live
  multicast demo; no Linux backend was built or tested for those, and
  consequently no ASan or TSan runs are committed for them. The NBBO/
  trade-through extension is portable standard C++ and does have committed
  ASan+UBSan runs (see above).
- The NBBO layer's "three simulated venues" are three independent random
  walks around one shared reference price, not three feeds of the same
  real symbol; locked and crossed markets never occurred naturally in the
  measured runs and are proven correct via explicit constructed cases
  instead (see Findings).
- The trade-through guard evaluates one order against the NBBO at the
  instant it is called; it does not model network latency between venues,
  so it cannot detect a trade-through caused by a stale NBBO view during
  the guard's own decision window.
- Reads through the binding while the feed is running are unsynchronized. They
  are useful for monitoring and wrong for anything that needs a consistent
  snapshot. A sequence-lock per book would fix this and is not implemented.
- The order book's fixed-size price-level array assumes prices stay within a
  pre-sized band around each instrument's reference price. A real system would
  need circuit-breaker-aware band sizing or a fallback path, not just a dropped
  update counter.
- The live multicast demo runs sender and receiver in one process on loopback.
  It exercises the real network, socket and decode path, not cross-host
  multicast routing, and the latency figures include no NIC or switch.
- The binding exposes aggregated depth, not individual resting orders. The
  `order_id -> location` map stays on the C++ side.
- The queue-position backtest's fill-rate and Sharpe targets were not
  reached after three genuine attempts (see Findings above), root-caused to
  `market_simulator.hpp`'s unbounded book growth and uniformly-random
  execution targeting, which does not concentrate order flow near the touch
  the way a real limit order book does. A generator built specifically for
  touch-concentrated flow would be a different, larger project.
- The backtest's Sharpe is a per-fill statistic over a single replay, not an
  annualized, multi-period risk-adjusted return, and the queue-tracked
  model's valid (non-crossed) sample size in the reported run is 2, too small
  to treat as a stable estimate.
