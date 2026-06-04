
from __future__ import annotations
import argparse
import math
import time
from collections import OrderedDict, Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from shared.dataset import DatasetStore, load_dataset
from shared.query_engine import QueryEngine
from shared.traffic import generate_requests

@dataclass
class CacheEntry:
    value: Any
    size: int
    inserted_at: float
    last_access: float
    freq: int = 1

class SimulatedCache:
    def __init__(self, capacity_bytes: int, policy: str, ttl_seconds: int):
        self.capacity_bytes = capacity_bytes
        self.policy = policy
        self.ttl_seconds = ttl_seconds
        self.entries: dict[str, CacheEntry] = {}
        self.order = OrderedDict()
        self.now = 0.0
        self.used_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _purge_expired(self):
        expired = [k for k, e in self.entries.items() if self.now - e.inserted_at >= self.ttl_seconds]
        for k in expired:
            self._remove(k)

    def _remove(self, key: str):
        if key in self.entries:
            self.used_bytes -= self.entries[key].size
            del self.entries[key]
            self.order.pop(key, None)

    def _evict_until_fit(self, incoming_size: int):
        while self.used_bytes + incoming_size > self.capacity_bytes and self.entries:
            if self.policy == "fifo":
                key = next(iter(self.order))
            elif self.policy == "lfu":
                key = min(self.entries.items(), key=lambda kv: (kv[1].freq, kv[1].last_access, kv[1].inserted_at))[0]
            else:
                key = next(iter(self.order))
            self._remove(key)
            self.evictions += 1

    def get(self, key: str):
        self._purge_expired()
        entry = self.entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        entry.last_access = self.now
        entry.freq += 1
        self.order.move_to_end(key)
        self.hits += 1
        return entry.value

    def put(self, key: str, value: Any):
        self._purge_expired()
        payload = json.dumps(value).encode("utf-8")
        size = max(600 * 1024, len(payload) * 8)
        if key in self.entries:
            self.used_bytes -= self.entries[key].size
            self.order.pop(key, None)
        self._evict_until_fit(size)
        e = CacheEntry(value=value, size=size, inserted_at=self.now, last_access=self.now, freq=1)
        self.entries[key] = e
        self.order[key] = None
        self.used_bytes += size

def canonical_key(req: dict) -> str:
    q = req["qtype"]
    if q == "Q1":
        return f"count:{req['zone_id']}:conf={req.get('confidence_min', 0.0):.2f}"
    if q == "Q2":
        return f"area:{req['zone_id']}:conf={req.get('confidence_min', 0.0):.2f}"
    if q == "Q3":
        return f"density:{req['zone_id']}:conf={req.get('confidence_min', 0.0):.2f}"
    if q == "Q4":
        a, b = sorted([req["zone_id_a"], req["zone_id_b"]])
        return f"compare:density:{a}:{b}:conf={req.get('confidence_min', 0.0):.2f}"
    return f"confidence_dist:{req['zone_id']}:bins={req.get('bins', 5)}"

def run_simulation(n=1000, distribution="zipf", policy="lru", capacity_mb=50, ttl=60, seed=42, interarrival_s=0.15):
    df = load_dataset("data/buildings_sample.csv")
    store = DatasetStore(df)
    engine = QueryEngine(store)
    cache = SimulatedCache(capacity_bytes=capacity_mb * 1024 * 1024, policy=policy, ttl_seconds=ttl)
    rows = []
    current_time = 0.0
    rng = np.random.default_rng(seed)
    requests = list(generate_requests(n, distribution=distribution, seed=seed))
    for req in requests:
        key = canonical_key(req)
        cache.now = current_time
        t0 = time.perf_counter()
        value = cache.get(key)
        if value is None:
            compute_t0 = time.perf_counter()
            value = engine.compute(req)
            compute_ms = (time.perf_counter() - compute_t0) * 1000
            cache.put(key, value)
            hit = False
            latency_ms = compute_ms + 1.7
        else:
            compute_ms = 0.0
            hit = True
            latency_ms = 0.9
        rows.append({
            "distribution": distribution,
            "policy": policy,
            "capacity_mb": capacity_mb,
            "ttl": ttl,
            "qtype": req["qtype"],
            "cache_hit": hit,
            "latency_ms": latency_ms,
            "compute_ms": compute_ms,
        })
        current_time += max(0.02, rng.exponential(interarrival_s))
    res = pd.DataFrame(rows)
    summary = {
        "distribution": distribution,
        "policy": policy,
        "capacity_mb": capacity_mb,
        "ttl": ttl,
        "requests": n,
        "hit_rate": float(res["cache_hit"].mean()),
        "miss_rate": float(1 - res["cache_hit"].mean()),
        "p50_ms": float(res["latency_ms"].quantile(0.5)),
        "p95_ms": float(res["latency_ms"].quantile(0.95)),
        "throughput_qps": float(n / current_time),
        "evictions": int(cache.evictions),
        "eviction_rate_per_min": float(cache.evictions / max(current_time / 60, 1e-6)),
        "used_bytes_end": int(cache.used_bytes),
        "avg_miss_ms": float(res.loc[~res["cache_hit"], "compute_ms"].mean() + 1.7 if (~res["cache_hit"]).any() else 0.0),
        "avg_hit_ms": float(res.loc[res["cache_hit"], "latency_ms"].mean() if res["cache_hit"].any() else 0.0),
    }
    return res, summary

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="report/figures")
    args = ap.parse_args()
    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    summaries = []
    all_frames = []

    for dist in ["zipf", "uniform"]:
        for policy in ["lru", "lfu", "fifo"]:
            for cap in [50, 200, 500]:
                df, s = run_simulation(n=1000, distribution=dist, policy=policy, capacity_mb=cap, ttl=60, seed=42)
                summaries.append(s)
                all_frames.append(df)
    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(out / "summary_policy_size.csv", index=False)

    overview = summary_df[(summary_df["policy"] == "lru") & (summary_df["capacity_mb"] == 200)].copy()
    fig, ax = plt.subplots(figsize=(7,4))
    ax.bar(overview["distribution"], overview["hit_rate"])
    ax.set_ylim(0, 1)
    ax.set_title("Hit rate por distribución")
    ax.set_ylabel("hit rate")
    fig.tight_layout()
    fig.savefig(out / "hit_rate_distribution.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8,4))
    for policy in ["lru", "lfu", "fifo"]:
        sub = summary_df[(summary_df["distribution"] == "zipf") & (summary_df["policy"] == policy)]
        ax.plot(sub["capacity_mb"], sub["hit_rate"], marker="o", label=policy.upper())
    ax.set_title("Impacto del tamaño de caché (Zipf)")
    ax.set_xlabel("MB")
    ax.set_ylabel("hit rate")
    ax.set_xticks([50, 200, 500])
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "hit_rate_cache_size.png", dpi=200)
    plt.close(fig)

    ttl_rows = []
    for ttl in [5, 15, 60, 300]:
        df, s = run_simulation(n=1000, distribution="zipf", policy="lru", capacity_mb=200, ttl=ttl, seed=42)
        s["ttl"] = ttl
        ttl_rows.append(s)
    ttl_df = pd.DataFrame(ttl_rows)
    ttl_df.to_csv(out / "ttl_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(8,4))
    ax.plot(ttl_df["ttl"], ttl_df["hit_rate"], marker="o")
    ax.set_title("Efecto del TTL (Zipf, LRU, 200MB)")
    ax.set_xlabel("TTL (s)")
    ax.set_ylabel("hit rate")
    ax.set_xticks([5,15,60,300])
    fig.tight_layout()
    fig.savefig(out / "ttl_hit_rate.png", dpi=200)
    plt.close(fig)

    rep, _ = run_simulation(n=1000, distribution="zipf", policy="lru", capacity_mb=200, ttl=60, seed=99)
    fig, ax = plt.subplots(figsize=(8,4))
    data = [rep[rep["qtype"] == q]["latency_ms"] for q in ["Q1","Q2","Q3","Q4","Q5"]]
    ax.boxplot(data, labels=["Q1","Q2","Q3","Q4","Q5"])
    ax.set_title("Latencia por tipo de consulta")
    ax.set_ylabel("ms")
    fig.tight_layout()
    fig.savefig(out / "latency_by_qtype.png", dpi=200)
    plt.close(fig)

    combo = summary_df.pivot_table(index=["distribution","policy"], columns="capacity_mb", values="hit_rate")
    combo.to_csv(out / "hit_rate_pivot.csv")

    print(summary_df.head().to_string())
    print("saved", out)

if __name__ == "__main__":
    main()
