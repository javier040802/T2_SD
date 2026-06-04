from __future__ import annotations
from dataclasses import dataclass
from itertools import cycle
from typing import List, Optional
import numpy as np

ZONE_IDS = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def zipf_zone_sampler(size: int, exponent: float = 1.25, seed: int = 42):
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, len(ZONE_IDS) + 1)
    probs = 1 / np.power(ranks, exponent)
    probs = probs / probs.sum()
    idx = rng.choice(len(ZONE_IDS), size=size, p=probs)
    return [ZONE_IDS[i] for i in idx]

def uniform_zone_sampler(size: int, seed: int = 42):
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(ZONE_IDS), size=size)
    return [ZONE_IDS[i] for i in idx]

def traffic_pattern(size: int, distribution: str = "zipf", seed: int = 42):
    if distribution == "zipf":
        return zipf_zone_sampler(size=size, seed=seed)
    if distribution == "uniform":
        return uniform_zone_sampler(size=size, seed=seed)
    raise ValueError("distribution must be zipf or uniform")

def random_query_from_zone(zone_id: str, rng: np.random.Generator):
    qtype = rng.choice(["Q1", "Q2", "Q3", "Q4", "Q5"], p=[0.25, 0.20, 0.20, 0.15, 0.20])
    confidence_min = round(float(rng.uniform(0.0, 0.6)), 2)
    if qtype == "Q1":
        return {"qtype": "Q1", "zone_id": zone_id, "confidence_min": confidence_min}
    if qtype == "Q2":
        return {"qtype": "Q2", "zone_id": zone_id, "confidence_min": confidence_min}
    if qtype == "Q3":
        return {"qtype": "Q3", "zone_id": zone_id, "confidence_min": confidence_min}
    if qtype == "Q4":
        other = rng.choice([z for z in ZONE_IDS if z != zone_id])
        return {"qtype": "Q4", "zone_id_a": zone_id, "zone_id_b": other, "confidence_min": confidence_min}
    return {"qtype": "Q5", "zone_id": zone_id, "bins": int(rng.choice([4, 5, 8, 10]))}

def generate_requests(n: int, distribution: str = "zipf", seed: int = 42):
    rng = np.random.default_rng(seed)
    zones = traffic_pattern(n, distribution=distribution, seed=seed)
    for zid in zones:
        yield random_query_from_zone(zid, rng)
