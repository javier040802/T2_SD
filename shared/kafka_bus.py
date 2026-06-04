from __future__ import annotations

import json
import time
from typing import Any, Iterable

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

from .config import settings


def json_dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def json_loads(value: bytes | None) -> Any:
    if value is None:
        return None
    return json.loads(value.decode("utf-8"))


def build_producer(client_id: str | None = None) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=client_id or f"{settings.kafka_client_prefix}-producer",
        acks="all",
        retries=5,
        linger_ms=10,
        value_serializer=json_dumps,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    )


def build_consumer(client_id: str, topics: Iterable[str]) -> KafkaConsumer:
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=client_id,
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=json_loads,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,
        max_poll_records=25,
    )
    return consumer


def create_topics(topics: list[tuple[str, int, int]], timeout_s: int = 60) -> list[str]:
    deadline = time.time() + timeout_s
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id=f"{settings.kafka_client_prefix}-admin",
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )
            existing = set(admin.list_topics())
            new_topics = [
                NewTopic(name=name, num_partitions=parts, replication_factor=replicas)
                for name, parts, replicas in topics
                if name not in existing
            ]
            if new_topics:
                admin.create_topics(new_topics, validate_only=False)
            admin.close()
            return [name for name, _, _ in topics]
        except Exception as exc:
            last_error = exc
            time.sleep(2)

    raise RuntimeError(f"Kafka topics were not created in time: {last_error}")
