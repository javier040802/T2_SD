from __future__ import annotations
from statistics import mean
from typing import Any, Dict
import numpy as np
from .dataset import DatasetStore, ZONE_AREA_KM2

class QueryEngine:
    def __init__(self, store: DatasetStore):
        self.store = store

    def q1_count(self, zone_id: str, confidence_min: float = 0.0) -> int:
        records = self.store.by_zone[zone_id]
        return int((records["confidence"] >= confidence_min).sum())

    def q2_area(self, zone_id: str, confidence_min: float = 0.0) -> Dict[str, Any]:
        records = self.store.by_zone[zone_id]
        filtered = records.loc[records["confidence"] >= confidence_min, "area_in_meters"]
        if len(filtered) == 0:
            return {"avg_area": 0.0, "total_area": 0.0, "n": 0}
        return {
            "avg_area": float(filtered.mean()),
            "total_area": float(filtered.sum()),
            "n": int(filtered.shape[0]),
        }

    def q3_density(self, zone_id: str, confidence_min: float = 0.0) -> float:
        count = self.q1_count(zone_id, confidence_min)
        return float(count / ZONE_AREA_KM2[zone_id])

    def q4_compare(self, zone_a: str, zone_b: str, confidence_min: float = 0.0) -> Dict[str, Any]:
        da = self.q3_density(zone_a, confidence_min)
        db = self.q3_density(zone_b, confidence_min)
        winner = zone_a if da > db else zone_b
        return {"zone_a": da, "zone_b": db, "winner": winner}

    def q5_confidence_dist(self, zone_id: str, bins: int = 5):
        scores = self.store.by_zone[zone_id]["confidence"].to_numpy()
        counts, edges = np.histogram(scores, bins=bins, range=(0, 1))
        return [
            {
                "bucket": int(i),
                "min": float(edges[i]),
                "max": float(edges[i + 1]),
                "count": int(counts[i]),
            }
            for i in range(bins)
        ]

    def compute(self, payload: dict):
        qtype = payload["qtype"]
        if qtype == "Q1":
            return self.q1_count(payload["zone_id"], payload.get("confidence_min", 0.0))
        if qtype == "Q2":
            return self.q2_area(payload["zone_id"], payload.get("confidence_min", 0.0))
        if qtype == "Q3":
            return self.q3_density(payload["zone_id"], payload.get("confidence_min", 0.0))
        if qtype == "Q4":
            return self.q4_compare(payload["zone_id_a"], payload["zone_id_b"], payload.get("confidence_min", 0.0))
        if qtype == "Q5":
            return self.q5_confidence_dist(payload["zone_id"], payload.get("bins", 5))
        raise ValueError(f"Unsupported qtype: {qtype}")
