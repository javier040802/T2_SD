from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, Field

from shared.models import MetricsSummary


STORE_PATH = Path("/data/metrics.jsonl")
SUMMARY_PATH = Path("/data/metrics_summary.json")

app = FastAPI(title="metrics-service")
events: list[dict[str, Any]] = []


class Event(BaseModel):
    timestamp: float
    request_id: str
    event_type: str
    qtype: str
    topic: str
    consumer_id: str = "producer"
    attempt: int = 0
    cache_hit: bool | None = None
    latency_ms: float = 0.0
    compute_ms: float = 0.0
    backlog_size: int = 0
    retry_delay_ms: int = 0
    response_status: str = "ok"
    created_at: float | None = None
    completed_at: float | None = None
    evicted_keys_total: int = 0
    redis_used_memory: int = 0
    redis_keyspace_hits: int = 0
    redis_keyspace_misses: int = 0
    source: str = "kafka"
    payload_size: int = 0
    extra: dict[str, Any] = Field(default_factory=dict)


def _ensure_store() -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)


def _append_event(event: dict[str, Any]) -> None:
    _ensure_store()
    with STORE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def _load_df() -> pd.DataFrame:
    if STORE_PATH.exists():
        rows = []
        with STORE_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    return pd.DataFrame(events)


def _safe_quantile(series: pd.Series, q: float) -> float:
    if series.empty:
        return 0.0
    return float(series.quantile(q))


def _summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return MetricsSummary().model_dump()

    duration = max(1.0, float(df["timestamp"].max()) - float(df["timestamp"].min()))
    success_df = df[df["event_type"].isin(["processed", "recovered"])].copy()
    generated = int(df.loc[df["event_type"] == "generated", "request_id"].nunique())
    retries = int(df.loc[df["event_type"] == "retry", "request_id"].count())
    failure_events = int(df.loc[df["event_type"] == "failure", "request_id"].count())
    recovered = int(df.loc[df["event_type"] == "recovered", "request_id"].nunique())
    dlq = int(df.loc[df["event_type"] == "dlq", "request_id"].nunique())
    processed = int(success_df["request_id"].nunique())
    hit_rate = float(success_df["cache_hit"].fillna(False).mean()) if not success_df.empty else 0.0
    backlog_size = int(df["backlog_size"].max()) if "backlog_size" in df.columns else 0

    if not success_df.empty:
        grouped = success_df.groupby("request_id")
        attempts_series = grouped["attempt"].max() + 1
        avg_attempts = float(attempts_series.mean())
    else:
        avg_attempts = 0.0

    recovery_times = []
    if "created_at" in df.columns:
        failure_times = (
            df.loc[df["event_type"].isin(["failure", "retry"]), ["request_id", "timestamp"]]
            .groupby("request_id")["timestamp"]
            .min()
        )
        for request_id, row in success_df.set_index("request_id").iterrows():
            if request_id in failure_times.index and row.get("attempt", 0) > 0:
                recovery_times.append(float(row["timestamp"] - failure_times.loc[request_id]) * 1000.0)
    recovery_time = float(pd.Series(recovery_times).mean()) if recovery_times else 0.0

    retry_base = max(1, generated)
    failure_base = max(1, failure_events + dlq + recovered)

    summary = MetricsSummary(
        count=int(df.shape[0]),
        generated=generated,
        processed=processed,
        recovered=recovered,
        retries=retries,
        failures=failure_events,
        dlq=dlq,
        hit_rate=hit_rate,
        retry_rate=float(retries / retry_base),
        recovery_rate=float(recovered / failure_base),
        dlq_rate=float(dlq / failure_base),
        latency_p50_ms=_safe_quantile(success_df["latency_ms"].astype(float), 0.50),
        latency_p95_ms=_safe_quantile(success_df["latency_ms"].astype(float), 0.95),
        throughput_qps=float(success_df.shape[0] / duration),
        backlog_size=backlog_size,
        recovery_time_ms=recovery_time,
        avg_attempts_per_processed=avg_attempts,
    )
    return summary.model_dump()


@app.post("/event")
def add_event(event: Event):
    payload = event.model_dump()
    events.append(payload)
    _append_event(payload)
    SUMMARY_PATH.write_text(json.dumps(_summary(pd.DataFrame(events)), indent=2), encoding="utf-8")
    return {"status": "accepted", "buffered": len(events)}


@app.get("/summary")
def summary():
    df = _load_df()
    s = _summary(df)
    SUMMARY_PATH.write_text(json.dumps(s, indent=2), encoding="utf-8")
    return s


@app.get("/events")
def get_events(limit: int = 100):
    df = _load_df()
    if df.empty:
        return {"events": []}
    return {"events": df.tail(limit).to_dict(orient="records")}


@app.get("/health")
def health():
    return {"status": "ok", "buffered": len(events)}
