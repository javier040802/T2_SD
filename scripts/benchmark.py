from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import requests
from kafka import KafkaProducer

from shared.config import settings
from shared.models import KafkaQueryMessage, TelemetryEvent
from shared.traffic import generate_requests


def _emit_generated(request_id: str, qtype: str, topic: str) -> None:
    try:
        requests.post(
            f"{settings.metrics_service_url}/event",
            json=TelemetryEvent(
                request_id=request_id,
                event_type="generated",
                qtype=qtype,
                topic=topic,
                consumer_id="benchmark",
                created_at=time.time(),
            ).model_dump(),
            timeout=3,
        )
    except Exception:
        pass


def _wait_for_summary(expected_count: int, timeout_s: int = 120) -> dict:
    deadline = time.time() + timeout_s
    last = {}
    while time.time() < deadline:
        try:
            r = requests.get(f"{settings.metrics_service_url}/summary", timeout=5)
            r.raise_for_status()
            last = r.json()
            processed_or_dlq = int(last.get("processed", 0)) + int(last.get("dlq", 0))
            if int(last.get("generated", 0)) >= expected_count and processed_or_dlq >= expected_count:
                return last
        except Exception:
            pass
        time.sleep(2)
    return last


def run_sync(url: str, distribution: str, n: int, seed: int) -> pd.DataFrame:
    rows = []
    for i, req in enumerate(generate_requests(n, distribution=distribution, seed=seed)):
        t0 = time.perf_counter()
        resp = requests.post(url, json=req, timeout=20)
        elapsed = (time.perf_counter() - t0) * 1000
        resp.raise_for_status()
        data = resp.json()
        rows.append(
            {
                "mode": "sync",
                "distribution": distribution,
                "idx": i,
                "qtype": req["qtype"],
                "cache_hit": data["cache_hit"],
                "latency_ms": data["response_ms"],
                "elapsed_ms": elapsed,
                "compute_ms": data["compute_ms"],
                "attempts": 0,
            }
        )
    return pd.DataFrame(rows)


def run_kafka(distribution: str, n: int, seed: int, delay_ms: int = 5) -> pd.DataFrame:
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )
    rows = []
    for i, req in enumerate(generate_requests(n, distribution=distribution, seed=seed)):
        msg = KafkaQueryMessage(**req).model_dump()
        msg["request_id"] = f"bench-{distribution}-{seed}-{i}-{int(time.time()*1000)}"
        msg["created_at"] = time.time()
        msg["available_at"] = time.time()
        t0 = time.perf_counter()
        producer.send(settings.kafka_main_topic, key=msg["request_id"], value=msg).get(timeout=15)
        _emit_generated(msg["request_id"], req["qtype"], settings.kafka_main_topic)
        elapsed = (time.perf_counter() - t0) * 1000
        rows.append(
            {
                "mode": "kafka",
                "distribution": distribution,
                "idx": i,
                "qtype": req["qtype"],
                "cache_hit": None,
                "latency_ms": None,
                "elapsed_ms": elapsed,
                "compute_ms": None,
                "attempts": 0,
            }
        )
        time.sleep(delay_ms / 1000.0)
    producer.flush(5)
    producer.close()
    return pd.DataFrame(rows)


def make_plots(df: pd.DataFrame, outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    if not df.empty and "latency_ms" in df:
        fig, ax = plt.subplots()
        for label, group in df.dropna(subset=["latency_ms"]).groupby("distribution"):
            ax.plot(group["idx"], group["latency_ms"].rolling(20, min_periods=1).mean(), label=label)
        ax.set_title("Latencia media móvil")
        ax.set_xlabel("request")
        ax.set_ylabel("ms")
        ax.legend()
        fig.tight_layout()
        fig.savefig(outdir / "latency_moving_avg.png", dpi=200)
        plt.close(fig)

        fig, ax = plt.subplots()
        hits = df[df["mode"] == "sync"].groupby("distribution")["cache_hit"].mean()
        hits.plot(kind="bar", ax=ax)
        ax.set_ylim(0, 1)
        ax.set_title("Hit rate por distribución (sync)")
        ax.set_ylabel("hit rate")
        fig.tight_layout()
        fig.savefig(outdir / "hit_rate_sync.png", dpi=200)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sync", "kafka"], default="sync")
    parser.add_argument("--url", default="http://localhost:8000/query")
    parser.add_argument("--distribution", choices=["zipf", "uniform"], default="zipf")
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--outdir", default="report/figures")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.mode == "sync":
        df = run_sync(args.url, args.distribution, args.n, args.seed)
    else:
        df = run_kafka(args.distribution, args.n, args.seed)

    df.to_csv(outdir / f"benchmark_{args.mode}_{args.distribution}.csv", index=False)
    make_plots(df, outdir)
    summary = _wait_for_summary(expected_count=args.n if args.mode == "kafka" else 0)
    if summary:
        (outdir / f"summary_{args.mode}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"rows": int(df.shape[0]), "outdir": str(outdir)}))


if __name__ == "__main__":
    main()
