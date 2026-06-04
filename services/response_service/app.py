from __future__ import annotations

import random
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.config import settings
from shared.dataset import DatasetStore, load_dataset
from shared.query_engine import QueryEngine


class ComputeRequest(BaseModel):
    qtype: str
    zone_id: str | None = None
    zone_id_a: str | None = None
    zone_id_b: str | None = None
    confidence_min: float = 0.0
    bins: int = 5


app = FastAPI(title="response-service")
store: DatasetStore | None = None
engine: QueryEngine | None = None

_fault_mode = {"enabled": False, "failure_rate": settings.response_failure_rate, "delay_ms": settings.response_delay_ms}


@app.on_event("startup")
def startup():
    global store, engine
    dataset_path = settings.dataset_path
    store = DatasetStore(load_dataset(dataset_path))
    engine = QueryEngine(store)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "dataset_rows": int(store.df.shape[0]) if store else 0,
        "fault_mode": _fault_mode,
    }


@app.get("/faults")
def read_faults():
    return _fault_mode


@app.post("/faults")
def update_faults(enabled: bool | None = None, failure_rate: float | None = None, delay_ms: int | None = None):
    if enabled is not None:
        _fault_mode["enabled"] = bool(enabled)
    if failure_rate is not None:
        _fault_mode["failure_rate"] = max(0.0, min(1.0, float(failure_rate)))
    if delay_ms is not None:
        _fault_mode["delay_ms"] = max(0, int(delay_ms))
    return _fault_mode


@app.post("/compute")
def compute(req: ComputeRequest):
    if engine is None:
        raise HTTPException(503, "engine not ready")

    mode_enabled = _fault_mode["enabled"] or _fault_mode["failure_rate"] > 0
    if _fault_mode["delay_ms"] > 0:
        time.sleep(_fault_mode["delay_ms"] / 1000.0)

    if mode_enabled and random.random() < _fault_mode["failure_rate"]:
        raise HTTPException(503, "simulated temporary failure")

    start = time.perf_counter()
    payload = engine.compute(req.model_dump())
    compute_ms = (time.perf_counter() - start) * 1000
    return {"payload": payload, "compute_ms": compute_ms}
