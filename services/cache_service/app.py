from __future__ import annotations

import json
import time
from uuid import uuid4

import redis
import requests
from fastapi import FastAPI, HTTPException

from shared.config import settings
from shared.models import QueryRequest


app = FastAPI(title="cache-service")
r = redis.Redis.from_url(settings.redis_url, decode_responses=True)


def cache_key(req: QueryRequest) -> str:
    if req.qtype in {"Q1", "Q2", "Q3", "Q5"}:
        if not req.zone_id:
            raise HTTPException(400, "zone_id is required")
        if req.qtype == "Q5":
            return f"confidence_dist:{req.zone_id}:bins={req.bins}"
        if req.qtype == "Q1":
            return f"count:{req.zone_id}:conf={req.confidence_min:.2f}"
        if req.qtype == "Q2":
            return f"area:{req.zone_id}:conf={req.confidence_min:.2f}"
        if req.qtype == "Q3":
            return f"density:{req.zone_id}:conf={req.confidence_min:.2f}"
    if req.qtype == "Q4":
        if not req.zone_id_a or not req.zone_id_b:
            raise HTTPException(400, "zone_id_a and zone_id_b are required")
        a, b = sorted([req.zone_id_a, req.zone_id_b])
        return f"compare:density:{a}:{b}:conf={req.confidence_min:.2f}"
    raise HTTPException(400, "invalid request")


def get_redis_info():
    info = r.info("stats")
    mem = r.info("memory")
    return {
        "evicted_keys_total": int(info.get("evicted_keys", 0)),
        "keyspace_hits": int(info.get("keyspace_hits", 0)),
        "keyspace_misses": int(info.get("keyspace_misses", 0)),
        "used_memory": int(mem.get("used_memory", 0)),
    }


def send_metric(event: dict):
    try:
        requests.post(f"{settings.metrics_service_url}/event", json=event, timeout=3)
    except Exception:
        pass


@app.get("/health")
def health():
    return {"status": "ok", "redis": bool(r.ping())}


@app.post("/query")
def query(req: QueryRequest):
    request_id = str(uuid4())
    key = cache_key(req)
    ttl = req.ttl_seconds or settings.ttl_seconds
    start = time.perf_counter()
    cached = r.get(key)
    compute_ms = 0.0
    if cached is not None:
        payload = json.loads(cached)
        cache_hit = True
    else:
        cache_hit = False
        body = req.model_dump()
        resp = requests.post(f"{settings.response_service_url}/compute", json=body, timeout=settings.request_timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        payload = data["payload"]
        compute_ms = float(data.get("compute_ms", 0.0))
        r.set(key, json.dumps(payload), ex=ttl)

    latency_ms = (time.perf_counter() - start) * 1000
    info = get_redis_info()
    send_metric(
        {
            "timestamp": time.time(),
            "request_id": request_id,
            "event_type": "processed",
            "qtype": req.qtype,
            "topic": "cache-service",
            "attempt": 0,
            "cache_hit": cache_hit,
            "latency_ms": latency_ms,
            "compute_ms": compute_ms,
            "response_status": "ok",
            "evicted_keys_total": info["evicted_keys_total"],
            "redis_used_memory": info["used_memory"],
            "redis_keyspace_hits": info["keyspace_hits"],
            "redis_keyspace_misses": info["keyspace_misses"],
            "source": "cache-service",
            "payload_size": len(json.dumps(payload).encode("utf-8")),
        }
    )
    return {
        "cache_key": key,
        "qtype": req.qtype,
        "zone_id": req.zone_id,
        "zone_id_a": req.zone_id_a,
        "zone_id_b": req.zone_id_b,
        "cache_hit": cache_hit,
        "ttl_seconds": ttl,
        "response_ms": latency_ms,
        "compute_ms": compute_ms,
        "payload": payload,
    }
