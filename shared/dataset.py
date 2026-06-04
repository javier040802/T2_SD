from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
import math
import pandas as pd

ZONES = {
    "Z1": {"name": "Providencia", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes", "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu", "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel", "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

def bbox_area_km2(zone: dict) -> float:
    lat_center = (zone["lat_min"] + zone["lat_max"]) / 2
    lat_km = abs(zone["lat_max"] - zone["lat_min"]) * 110.574
    lon_km = abs(zone["lon_max"] - zone["lon_min"]) * 111.320 * math.cos(math.radians(lat_center))
    return lat_km * lon_km

ZONE_AREA_KM2 = {zid: bbox_area_km2(z) for zid, z in ZONES.items()}

def generate_sample_dataset(path: str | Path, seed: int = 42, n_per_zone: Dict[str, int] | None = None) -> pd.DataFrame:
    import numpy as np
    rng = np.random.default_rng(seed)
    if n_per_zone is None:
        n_per_zone = {"Z1": 12000, "Z2": 18000, "Z3": 15000, "Z4": 20000, "Z5": 14000}
    rows = []
    for zid, zone in ZONES.items():
        n = n_per_zone.get(zid, 10000)
        lat = rng.uniform(zone["lat_min"], zone["lat_max"], size=n)
        lon = rng.uniform(zone["lon_min"], zone["lon_max"], size=n)
        mix = {
            "Z1": (80, 22),
            "Z2": (110, 35),
            "Z3": (60, 18),
            "Z4": (95, 30),
            "Z5": (70, 20),
        }[zid]
        area = np.clip(rng.lognormal(mean=math.log(mix[0]), sigma=0.55, size=n), 10, 5000)
        conf = np.clip(rng.beta(mix[1], 6, size=n), 0, 1)
        rows.append(pd.DataFrame({"zone_id": zid, "latitude": lat, "longitude": lon, "area_in_meters": area, "confidence": conf}))
    df = pd.concat(rows, ignore_index=True)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df

def load_dataset(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return generate_sample_dataset(path)
    df = pd.read_csv(path)
    expected = {"zone_id", "latitude", "longitude", "area_in_meters", "confidence"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Dataset missing columns: {missing}")
    return df

class DatasetStore:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.by_zone = {zid: g.reset_index(drop=True) for zid, g in df.groupby("zone_id")}
    @classmethod
    def from_path(cls, path: str | Path):
        return cls(load_dataset(path))
