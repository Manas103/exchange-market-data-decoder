"""Turns backtest_cli's flat CSV of finished orders into fill-rate and Sharpe
numbers for the naive and queue-tracked fill models.

P&L model (deliberately simple, stated here so the numbers below are
falsifiable): a filled order is treated as one leg of a round-trip market-
making trade. Gross P&L per fill is the full quoted spread at the moment of
the fill, (ask_at_fill - bid_at_fill) * qty, the idealized case of capturing
both sides of the spread with no execution cost on the exit leg. Net P&L
subtracts a flat exchange fee (0.05 cents/share, a representative equities
maker/taker fee) charged on both legs of the round trip, and gives back half
the spread to model the realistic cost of exiting the position by crossing
rather than resting again: net = 0.5 * spread_at_fill * qty - 2 * fee * qty.
Ticks are $0.01. This is a model choice, not a claim about any specific
venue's fee schedule; see the README "what this is NOT" section.

Usage: analyze_backtest.py <results.csv>
"""
import csv
import math
import statistics
import sys

TICK = 0.01
FEE_PER_SHARE = 0.0005


def sharpe(pnls):
    if len(pnls) < 2:
        return float("nan")
    mean = statistics.mean(pnls)
    stdev = statistics.pstdev(pnls)
    if stdev == 0:
        return float("nan")
    return mean / stdev


def main():
    path = sys.argv[1]
    rows = {"naive": [], "queue": []}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows[r["model"]].append(r)

    print(f"{'':24s}{'naive':>12s}{'queue-tracked':>16s}")
    fills = {}
    for model in ("naive", "queue"):
        model_rows = rows[model]
        total = len(model_rows)
        filled = [r for r in model_rows if r["outcome"] == "FILLED"]
        fills[model] = filled
        rate = 100.0 * len(filled) / total if total else 0.0
        print(f"{'orders':24s}{total:12d}{len(filled):16d}  (filled)")
        print(f"{'fill rate':24s}{rate:11.2f}%")

    print()
    print("Note: the base repo's OrderBook is fed a synthetic stream with no matching")
    print("logic (see README, 'what this is NOT'), so it is frequently crossed")
    print("(best bid >= best ask) at an arbitrary instant; a crossed quote has no")
    print("economically meaningful spread to capture, so P&L is computed only over")
    print("fills observed while the book was NOT crossed. The crossed fraction is")
    print("reported because it is itself the finding: this repo is a decoder and")
    print("normalizer, not a matching engine, and never claimed to keep a crossless")
    print("book.")
    print()
    for model in ("naive", "queue"):
        filled = fills[model]
        crossed = [r for r in filled if int(r["bid_at_fill"]) >= int(r["ask_at_fill"])]
        valid = [r for r in filled if int(r["bid_at_fill"]) < int(r["ask_at_fill"])]
        gross_pnls, net_pnls = [], []
        for r in valid:
            qty = int(r["qty"])
            spread = (int(r["ask_at_fill"]) - int(r["bid_at_fill"])) * TICK
            gross = spread * qty
            net = 0.5 * spread * qty - 2 * FEE_PER_SHARE * qty
            gross_pnls.append(gross)
            net_pnls.append(net)
        gs = sharpe(gross_pnls)
        ns = sharpe(net_pnls)
        crossed_pct = 100.0 * len(crossed) / len(filled) if filled else 0.0
        print(f"{model:24s} n_fills={len(filled):4d}  crossed_at_fill={len(crossed):4d} "
              f"({crossed_pct:.1f}%)  n_valid={len(valid):4d}")
        print(f"{'':24s} gross_sharpe={gs:.4f}  net_sharpe={ns:.4f}  "
              f"gross_sum=${sum(gross_pnls):.2f}  net_sum=${sum(net_pnls):.2f}")


if __name__ == "__main__":
    main()
