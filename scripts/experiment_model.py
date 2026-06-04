from __future__ import annotations

import json
import heapq
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def service_time_ms(cache_hit: bool) -> float:
    return 8.0 if cache_hit else 60.0


def generate_arrivals(n: int, base_rate: float, spike_rate: float, spike_start: float, spike_end: float, spike: bool, seed: int = 42):
    rng = np.random.default_rng(seed)
    arrivals = []
    t = 0.0
    for _ in range(n):
        rate = spike_rate if (spike and spike_start <= t < spike_end) else base_rate
        t += rng.exponential(1.0 / rate)
        arrivals.append(t)
    return arrivals


def simulate_queue(
    n: int = 600,
    consumers: int = 1,
    distribution: str = "zipf",
    failure_rate: float = 0.0,
    spike: bool = False,
    seed: int = 42,
    max_retries: int = 2,
    base_rate: float = 16.0,
    spike_rate: float = 30.0,
):
    rng = np.random.default_rng(seed)
    hit_prob = 0.68 if distribution == "zipf" else 0.42
    arrivals = generate_arrivals(n, base_rate=base_rate, spike_rate=spike_rate, spike_start=15.0, spike_end=25.0, spike=spike, seed=seed)

    # Future jobs as min-heap by availability time.
    future: list[tuple[float, int, float, int, float | None]] = []
    for idx, created_at in enumerate(arrivals):
        heapq.heappush(future, (created_at, idx, created_at, 0, None))

    # Workers represented by their next-free times.
    workers = [0.0] * consumers
    heapq.heapify(workers)

    ready: list[tuple[float, int, float, int, float | None]] = []
    latencies: list[float] = []
    backlog_max = 0
    last_finish = 0.0
    recovered = 0
    failures = 0
    dlq = 0
    recovery_times: list[float] = []
    current_time = 0.0
    seq = n

    while future or ready:
        # Move available future jobs into ready queue.
        while future and future[0][0] <= current_time:
            ready.append(heapq.heappop(future))

        # If nothing is ready, jump to next future availability or next worker availability.
        if not ready:
            next_future = future[0][0] if future else float("inf")
            next_worker = workers[0] if workers else float("inf")
            current_time = next_future if future else next_worker
            while future and future[0][0] <= current_time:
                ready.append(heapq.heappop(future))
            if not ready:
                continue

        # If no worker is free yet, advance time to next worker free.
        if workers and workers[0] > current_time:
            current_time = workers[0]
            while future and future[0][0] <= current_time:
                ready.append(heapq.heappop(future))

        # Assign as many jobs as we can at this time.
        while ready and workers and workers[0] <= current_time:
            worker_free = heapq.heappop(workers)
            available_at, _, created_at, attempts, first_failure_at = ready.pop(0)
            start_t = max(current_time, worker_free, available_at)
            cache_hit = rng.random() < hit_prob
            finish_t = start_t + service_time_ms(cache_hit) / 1000.0

            last_finish = max(last_finish, finish_t)
            if rng.random() < failure_rate:
                failures += 1
                if attempts < max_retries:
                    delay = [1.0, 5.0, 10.0][min(attempts, 2)]
                    heapq.heappush(future, (finish_t + delay, seq, created_at, attempts + 1, first_failure_at or finish_t))
                    seq += 1
                else:
                    dlq += 1
            else:
                latency = (finish_t - created_at) * 1000.0
                latencies.append(latency)
                if attempts > 0:
                    recovered += 1
                    recovery_times.append((finish_t - (first_failure_at or created_at)) * 1000.0)

            heapq.heappush(workers, finish_t)

        backlog_max = max(backlog_max, len(ready))
        if future:
            current_time = min(future[0][0], workers[0] if workers else float("inf"))
        elif ready:
            current_time = workers[0] if workers else current_time

    duration = max(last_finish, max(arrivals) if arrivals else 1.0)
    throughput = len(latencies) / duration
    p50 = float(np.quantile(latencies, 0.50)) if latencies else 0.0
    p95 = float(np.quantile(latencies, 0.95)) if latencies else 0.0
    recovery_rate = recovered / max(1, failures)
    dlq_rate = dlq / max(1, failures)
    recovery_time = float(np.mean(recovery_times)) if recovery_times else 0.0

    return {
        "latencies": np.array(latencies),
        "throughput_qps": throughput,
        "p50_ms": p50,
        "p95_ms": p95,
        "backlog_max": backlog_max,
        "dlq_rate": dlq_rate,
        "recovery_rate": recovery_rate,
        "recovery_time_ms": recovery_time,
        "processed": len(latencies),
        "failures": failures,
        "dlq": dlq,
        "recovered": recovered,
    }


def build_figures(outdir: Path) -> pd.DataFrame:
    outdir.mkdir(parents=True, exist_ok=True)
    scenarios = [
        ("Base sync", simulate_queue(consumers=1, distribution="zipf", failure_rate=0.0, seed=1)),
        ("Kafka 1 consumer", simulate_queue(consumers=1, distribution="zipf", failure_rate=0.02, seed=2)),
        ("Kafka 3 consumers", simulate_queue(consumers=3, distribution="zipf", failure_rate=0.02, seed=3)),
        ("Failure window", simulate_queue(consumers=3, distribution="zipf", failure_rate=0.10, seed=4)),
        ("Spike traffic", simulate_queue(consumers=3, distribution="uniform", failure_rate=0.02, spike=True, seed=5)),
    ]

    rows = []
    for name, s in scenarios:
        rows.append(
            {
                "scenario": name,
                "throughput_qps": s["throughput_qps"],
                "p50_ms": s["p50_ms"],
                "p95_ms": s["p95_ms"],
                "backlog_max": s["backlog_max"],
                "dlq_rate": s["dlq_rate"],
                "recovery_rate": s["recovery_rate"],
                "recovery_time_ms": s["recovery_time_ms"],
            }
        )
    df = pd.DataFrame(rows)
    df.to_csv(outdir / "experiment_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(df["scenario"], df["throughput_qps"])
    ax.set_ylabel("req/s")
    ax.set_title("Throughput por escenario")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(outdir / "throughput_by_scenario.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(df["scenario"], df["p95_ms"])
    ax.set_ylabel("ms")
    ax.set_title("Latencia p95 por escenario")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(outdir / "p95_by_scenario.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(df["scenario"], df["backlog_max"])
    ax.set_ylabel("mensajes")
    ax.set_title("Backlog máximo por escenario")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(outdir / "backlog_by_scenario.png", dpi=200)
    plt.close(fig)


    scaling_rows = []
    for consumers in [1, 2, 3, 4]:
        s = simulate_queue(
            consumers=consumers,
            distribution="zipf",
            failure_rate=0.02,
            seed=11,
            base_rate=30.0,
            spike_rate=30.0,
        )
        scaling_rows.append({"consumers": consumers, "throughput_qps": s["throughput_qps"], "p95_ms": s["p95_ms"], "backlog_max": s["backlog_max"]})
    scaling_df = pd.DataFrame(scaling_rows)
    scaling_df.to_csv(outdir / "consumer_scaling.csv", index=False)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(scaling_df["consumers"], scaling_df["throughput_qps"], marker="o")
    ax.set_xlabel("consumers")
    ax.set_ylabel("req/s")
    ax.set_title("Throughput con múltiples consumidores")
    ax.set_xticks([1, 2, 3, 4])
    fig.tight_layout()
    fig.savefig(outdir / "throughput_by_consumers.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(df["backlog_max"], df["p95_ms"])
    for _, row in df.iterrows():
        ax.annotate(row["scenario"], (row["backlog_max"], row["p95_ms"]), fontsize=8)
    ax.set_xlabel("backlog máximo")
    ax.set_ylabel("p95 ms")
    ax.set_title("Relación backlog vs latencia")
    fig.tight_layout()
    fig.savefig(outdir / "backlog_vs_latency.png", dpi=200)
    plt.close(fig)

    return df


def main():
    outdir = Path("report/figures")
    df = build_figures(outdir)
    print(df.to_string(index=False))
    print(json.dumps({"outdir": str(outdir)}))


if __name__ == "__main__":
    main()
