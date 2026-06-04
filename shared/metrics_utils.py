from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List
import json
import time
import pandas as pd

def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return float(s[idx])

class MetricsAggregator:
    def __init__(self):
        self.events: list[dict] = []

    def add(self, event: dict):
        self.events.append(event)

    def dataframe(self):
        if not self.events:
            return pd.DataFrame()
        return pd.DataFrame(self.events)

    def summary(self) -> dict:
        df = self.dataframe()
        if df.empty:
            return {"count": 0}
        hits = int(df["cache_hit"].sum())
        misses = int((~df["cache_hit"]).sum())
        lat = df["latency_ms"].astype(float).tolist()
        compute = df["compute_ms"].astype(float).tolist()
        return {
            "count": int(df.shape[0]),
            "hits": hits,
            "misses": misses,
            "hit_rate": hits / max(1, hits + misses),
            "miss_rate": misses / max(1, hits + misses),
            "latency_p50_ms": percentile(lat, 50),
            "latency_p95_ms": percentile(lat, 95),
            "compute_p50_ms": percentile(compute, 50),
            "compute_p95_ms": percentile(compute, 95),
        }

def metrics_from_csv(path: str):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)
