from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Tuple


def _csv_env(name: str, default: str) -> Tuple[str, ...]:
    raw = os.getenv(name, default)
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    response_service_url: str = os.getenv("RESPONSE_SERVICE_URL", "http://response-service:8001")
    metrics_service_url: str = os.getenv("METRICS_SERVICE_URL", "http://metrics-service:8002")
    cache_service_url: str = os.getenv("CACHE_SERVICE_URL", "http://cache-service:8000")
    dataset_path: str = os.getenv("DATASET_PATH", "/app/data/buildings_sample.csv")
    ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "60"))
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_main_topic: str = os.getenv("KAFKA_MAIN_TOPIC", "queries.main")
    kafka_retry_topics: Tuple[str, ...] = _csv_env(
        "KAFKA_RETRY_TOPICS",
        "queries.retry.1s,queries.retry.5s",
    )
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "queries.dlq")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "sd-task2-consumers")
    kafka_client_prefix: str = os.getenv("KAFKA_CLIENT_PREFIX", "sd-task2")

    max_retries: int = int(os.getenv("MAX_RETRIES", str(len(_csv_env("KAFKA_RETRY_TOPICS", "queries.retry.1s,queries.retry.5s")))))
    retry_delays_seconds: Tuple[int, ...] = tuple(
        int(x) for x in _csv_env("RETRY_DELAYS_SECONDS", "1,5")
    )

    response_delay_ms: int = int(os.getenv("RESPONSE_DELAY_MS", "0"))
    response_failure_rate: float = float(os.getenv("RESPONSE_FAILURE_RATE", "0.0"))

    consumer_poll_ms: int = int(os.getenv("KAFKA_CONSUMER_POLL_MS", "1000"))
    consumer_backlog_window: int = int(os.getenv("KAFKA_BACKLOG_WINDOW", "20"))
    app_port: int = int(os.getenv("PORT", "8000"))


settings = Settings()
