# Exchange Market Data Decoder, Order Book Normalizer, and Zero-Copy Python Interface

A C++17 decoder for a simulated exchange's binary multicast feed, modeled on the
core ideas behind Nasdaq-style ITCH protocols: a compact, fixed-width message
stream describing order-book events (add, execute, cancel, delete, replace),
decoded into full-depth limit order books for 512 instruments by a sharded,
multithreaded normalizer, with a nanobind extension that hands Python live
full-depth views of those books without copying a byte. Every number in this
README was measured on the machine described below, not targeted. The one
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

## Layout

See the Architecture section.

## Limitations

- Simplified ITCH-style protocol, not the real Nasdaq ITCH 5.0 spec.
- Synthetic order flow, not a captured real exchange feed.
- Windows and Winsock only network layer; no Linux backend was built or tested,
  and consequently no ASan or TSan runs are committed for this repo.
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
