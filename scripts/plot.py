#!/usr/bin/env python3
"""Plot results/netem.csv and results/capacity.csv into results/*.png.

usage: python3 scripts/plot.py [results_dir]      (needs matplotlib)
"""
import csv
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent.parent / "results"
COLORS = {"tcp": "#2a6fdb", "udp": "#e8743b"}


def load(name):
    path = RESULTS / name
    if not path.exists():
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def by_proto(rows, proto):
    return [r for r in rows if r["proto"] == proto]


def loss_of(row):
    return float(row["label"].removeprefix("loss"))


def plot_netem(rows):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    losses = sorted({loss_of(r) for r in rows})

    ax = axes[0]
    for proto in ("tcp", "udp"):
        rs = sorted(by_proto(rows, proto), key=loss_of)
        ax.plot([loss_of(r) for r in rs], [float(r["delivery_pct"]) for r in rs],
                "o-", color=COLORS[proto], label=proto.upper())
    ax.plot(losses, [100 * (1 - p / 100) ** 2 for p in losses], "k--", lw=1,
            label="(1-p)² — two lossy hops")
    ax.set(title="Messages delivered", xlabel="netem loss on lo (%)",
           ylabel="delivered (%)")
    ax.legend()
    ax.grid(alpha=0.3)

    ax = axes[1]
    for proto in ("tcp", "udp"):
        rs = sorted(by_proto(rows, proto), key=loss_of)
        x = [loss_of(r) for r in rs]
        ax.plot(x, [float(r["lat_p50_us"]) / 1e3 for r in rs], "o-",
                color=COLORS[proto], label=f"{proto.upper()} p50")
        ax.plot(x, [float(r["lat_p99_us"]) / 1e3 for r in rs], "s--",
                color=COLORS[proto], label=f"{proto.upper()} p99")
    ax.set_yscale("log")
    ax.set(title="One-way message latency", xlabel="netem loss on lo (%)",
           ylabel="latency (ms, log scale)")
    ax.legend()
    ax.grid(alpha=0.3, which="both")

    ax = axes[2]
    for proto in ("tcp", "udp"):
        rs = sorted(by_proto(rows, proto), key=loss_of)
        ax.plot([loss_of(r) for r in rs], [float(r["udp_reorder_pct"]) for r in rs],
                "o-", color=COLORS[proto], label=proto.upper() +
                (" (in-order by design)" if proto == "tcp" else " (server→client seq)"))
    ax.set(title="Datagrams arriving out of order", xlabel="netem loss on lo (%)",
           ylabel="reordered (%)")
    ax.legend()
    ax.grid(alpha=0.3)

    r0 = rows[0]
    fig.suptitle(f"TCP vs UDP under packet loss — {r0['clients']} clients, "
                 f"{r0['rooms']} rooms, {r0['target_rate']} msg/s, netem delay + jitter")
    fig.tight_layout()
    fig.savefig(RESULTS / "netem.png", dpi=130)
    print("wrote", RESULTS / "netem.png")


def plot_capacity(rows):
    rows = [r for r in rows if r["label"].startswith("load")]
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    for proto in ("tcp", "udp"):
        rs = sorted(by_proto(rows, proto), key=lambda r: float(r["target_rate"]))
        x = [float(r["deliver_rate"]) / 1e3 for r in rs]
        ax.plot(x, [float(r["lat_p50_us"]) / 1e3 for r in rs], "o-",
                color=COLORS[proto], label=f"{proto.upper()} p50")
        ax.plot(x, [float(r["lat_p99_us"]) / 1e3 for r in rs], "s--",
                color=COLORS[proto], label=f"{proto.upper()} p99")
    ax.set_yscale("log")
    ax.set(title=f"Latency vs load — {rows[0]['clients']} clients, single-threaded server",
           xlabel="deliveries per second (thousands)", ylabel="latency (ms, log scale)")
    ax.legend()
    ax.grid(alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(RESULTS / "capacity.png", dpi=130)
    print("wrote", RESULTS / "capacity.png")


def ms(row, key):
    return f"{float(row[key]) / 1e3:.1f}"


def print_tables(netem, capacity):
    """Markdown tables for the README, straight from the CSVs."""
    if netem:
        print("\n| loss | proto | delivered | p50 ms | p95 ms | p99 ms | max ms | reordered |")
        print("|---|---|---|---|---|---|---|---|")
        for r in sorted(netem, key=lambda r: (loss_of(r), r["proto"])):
            reo = "0 (in-order)" if r["proto"] == "tcp" else f"{float(r['udp_reorder_pct']):.2f}%"
            print(f"| {loss_of(r):g}% | {r['proto'].upper()} | {float(r['delivery_pct']):.2f}% "
                  f"| {ms(r, 'lat_p50_us')} | {ms(r, 'lat_p95_us')} | {ms(r, 'lat_p99_us')} "
                  f"| {ms(r, 'lat_max_us')} | {reo} |")
    if capacity:
        print("\n| run | proto | clients | msg/s | deliveries/s | delivered | p50 ms | p99 ms |")
        print("|---|---|---|---|---|---|---|---|")
        for r in capacity:
            print(f"| {r['label']} | {r['proto'].upper()} | {r['clients']} | {float(r['send_rate']):.0f} "
                  f"| {float(r['deliver_rate']):,.0f} | {float(r['delivery_pct']):.2f}% "
                  f"| {ms(r, 'lat_p50_us')} | {ms(r, 'lat_p99_us')} |")


if __name__ == "__main__":
    netem, capacity = load("netem.csv"), load("capacity.csv")
    if netem:
        plot_netem(netem)
    plot_capacity(capacity)
    print_tables(netem, capacity)
