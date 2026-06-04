from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

from shared.config import settings
from shared.kafka_bus import build_producer
from shared.models import KafkaQueryMessage, TelemetryEvent
from shared.traffic import generate_requests


def _emit(event: dict):
    try:
        requests.post(f"{settings.metrics_service_url}/event", json=event, timeout=3)
    except Exception:
        pass


def _send_sync(req: dict) -> None:
    resp = requests.post(f"{settings.cache_service_url}/query", json=req, timeout=settings.request_timeout_seconds)
    resp.raise_for_status()


def _send_kafka(producer, req: dict, topic: str) -> None:
    msg = KafkaQueryMessage(**req)
    producer.send(topic, key=msg.request_id, value=msg.model_dump()).get(timeout=15)


def run(
    mode: str = "kafka",
    distribution: str = "zipf",
    requests_count: int = 200,
    delay_ms: int = 20,
    cycles: int = 1,
    spike_after: int | None = None,
    spike_duration: int = 0,
    spike_multiplier: int = 5,
    seed: int = 42,
) -> None:
    producer = build_producer(client_id=f"{settings.kafka_client_prefix}-traffic") if mode == "kafka" else None
    sent = 0
    try:
        for cycle in range(cycles):
            for idx, req in enumerate(generate_requests(requests_count, distribution=distribution, seed=seed + cycle)):
                effective_delay_ms = delay_ms
                if spike_after is not None and spike_after <= idx < spike_after + spike_duration:
                    effective_delay_ms = max(1, int(delay_ms / max(1, spike_multiplier)))
                msg = dict(req)
                msg["ttl_seconds"] = msg.get("ttl_seconds") or settings.ttl_seconds
                request_id = f"gen-{cycle}-{idx}-{int(time.time()*1000)}"
                event = TelemetryEvent(
                    request_id=request_id,
                    event_type="generated",
                    qtype=msg["qtype"],
                    topic=settings.kafka_main_topic if mode == "kafka" else "sync",
                    consumer_id="traffic-generator",
                    created_at=time.time(),
                ).model_dump()
                _emit(event)
                if mode == "kafka":
                    msg["request_id"] = request_id
                    msg["created_at"] = time.time()
                    msg["available_at"] = time.time()
                    msg["attempts"] = 0
                    msg["origin_topic"] = settings.kafka_main_topic
                    _send_kafka(producer, msg, settings.kafka_main_topic)
                else:
                    _send_sync(msg)
                sent += 1
                time.sleep(effective_delay_ms / 1000.0)
    finally:
        if producer is not None:
            try:
                producer.flush(5)
                producer.close()
            except Exception:
                pass
    print(json.dumps({"mode": mode, "sent": sent, "distribution": distribution}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["kafka", "sync"], default="kafka")
    parser.add_argument("--distribution", choices=["zipf", "uniform"], default="zipf")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--delay-ms", type=int, default=20)
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--spike-after", type=int, default=None)
    parser.add_argument("--spike-duration", type=int, default=0)
    parser.add_argument("--spike-multiplier", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(
        mode=args.mode,
        distribution=args.distribution,
        requests_count=args.requests,
        delay_ms=args.delay_ms,
        cycles=args.cycles,
        spike_after=args.spike_after,
        spike_duration=args.spike_duration,
        spike_multiplier=args.spike_multiplier,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
