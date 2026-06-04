from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

import requests
from fastapi import FastAPI
from kafka import errors as kafka_errors

from shared.config import settings
from shared.kafka_bus import build_consumer, build_producer
from shared.models import KafkaQueryMessage, QueryRequest, TelemetryEvent


app = FastAPI(title="kafka-consumer-service")
stop_event = threading.Event()
worker_thread: threading.Thread | None = None

state = {
    "consumer_id": os.getenv("CONSUMER_ID", os.getenv("HOSTNAME", "consumer-1")),
    "processed": 0,
    "retries": 0,
    "dlq": 0,
    "failures": 0,
    "last_backlog": 0,
    "last_message_at": None,
}


def _emit(event: dict[str, Any]) -> None:
    try:
        requests.post(f"{settings.metrics_service_url}/event", json=event, timeout=4)
    except Exception:
        pass


def _estimate_backlog(consumer) -> int:
    try:
        assignment = consumer.assignment()
        if not assignment:
            return 0
        end_offsets = consumer.end_offsets(assignment)
        backlog = 0
        for tp in assignment:
            try:
                pos = consumer.position(tp)
                backlog += max(0, int(end_offsets.get(tp, 0)) - int(pos))
            except Exception:
                continue
        return int(backlog)
    except Exception:
        return 0


def _publish(producer, topic: str, payload: dict[str, Any], key: str) -> None:
    producer.send(topic, key=key, value=payload).get(timeout=15)
    producer.flush(5)


def _process_message(msg, producer, consumer) -> None:
    payload = msg.value
    topic = msg.topic
    request = KafkaQueryMessage.model_validate(payload)
    query = QueryRequest.model_validate(request.to_query_request().model_dump())
    state["last_message_at"] = time.time()

    if topic in settings.kafka_retry_topics:
        delay = max(0.0, request.available_at - time.time())
        if delay > 0:
            time.sleep(delay)

    start = time.perf_counter()
    try:
        response = requests.post(
            f"{settings.cache_service_url}/query",
            json=query.model_dump(),
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        latency_ms = float(data.get("response_ms", (time.perf_counter() - start) * 1000))
        compute_ms = float(data.get("compute_ms", 0.0))
        cache_hit = bool(data.get("cache_hit", False))
        backlog = _estimate_backlog(consumer)
        state["last_backlog"] = backlog
        event_type = "recovered" if request.attempts > 0 else "processed"
        if event_type == "recovered":
            state["processed"] += 1
        else:
            state["processed"] += 1

        _emit(
            TelemetryEvent(
                request_id=request.request_id,
                event_type=event_type,
                qtype=request.qtype,
                topic=topic,
                consumer_id=state["consumer_id"],
                attempt=request.attempts,
                cache_hit=cache_hit,
                latency_ms=latency_ms,
                compute_ms=compute_ms,
                backlog_size=backlog,
                response_status="ok",
                created_at=request.created_at,
                completed_at=time.time(),
            ).model_dump()
        )
    except Exception as exc:
        state["failures"] += 1
        backlog = _estimate_backlog(consumer)
        state["last_backlog"] = backlog

        _emit(
            TelemetryEvent(
                request_id=request.request_id,
                event_type="failure",
                qtype=request.qtype,
                topic=topic,
                consumer_id=state["consumer_id"],
                attempt=request.attempts,
                backlog_size=backlog,
                response_status=str(exc),
                created_at=request.created_at,
                completed_at=time.time(),
            ).model_dump()
        )

        next_attempt = request.attempts + 1
        can_retry = next_attempt <= settings.max_retries and next_attempt <= len(settings.kafka_retry_topics)
        if can_retry:
            retry_topic = settings.kafka_retry_topics[request.attempts]
            delay_s = settings.retry_delays_seconds[min(request.attempts, len(settings.retry_delays_seconds) - 1)]
            retry_payload = request.model_dump()
            retry_payload["attempts"] = next_attempt
            retry_payload["origin_topic"] = topic
            retry_payload["available_at"] = time.time() + delay_s
            _publish(producer, retry_topic, retry_payload, key=request.request_id)
            state["retries"] += 1
            _emit(
                TelemetryEvent(
                    request_id=request.request_id,
                    event_type="retry",
                    qtype=request.qtype,
                    topic=topic,
                    consumer_id=state["consumer_id"],
                    attempt=request.attempts,
                    backlog_size=backlog,
                    retry_delay_ms=int(delay_s * 1000),
                    response_status=str(exc),
                    created_at=request.created_at,
                    completed_at=time.time(),
                ).model_dump()
            )
        else:
            dlq_payload = request.model_dump()
            dlq_payload["origin_topic"] = topic
            _publish(producer, settings.kafka_dlq_topic, dlq_payload, key=request.request_id)
            state["dlq"] += 1
            _emit(
                TelemetryEvent(
                    request_id=request.request_id,
                    event_type="dlq",
                    qtype=request.qtype,
                    topic=topic,
                    consumer_id=state["consumer_id"],
                    attempt=request.attempts,
                    backlog_size=backlog,
                    response_status=str(exc),
                    created_at=request.created_at,
                    completed_at=time.time(),
                ).model_dump()
            )


def _run_worker() -> None:
    topics = [settings.kafka_main_topic, *settings.kafka_retry_topics]
    while not stop_event.is_set():
        consumer = None
        producer = None
        try:
            consumer = build_consumer(client_id=f"{settings.kafka_client_prefix}-{state['consumer_id']}", topics=topics)
            producer = build_producer(client_id=f"{settings.kafka_client_prefix}-{state['consumer_id']}")
            while not stop_event.is_set():
                records = consumer.poll(timeout_ms=settings.consumer_poll_ms, max_records=25)
                if not records:
                    continue
                for _tp, batch in records.items():
                    for msg in batch:
                        _process_message(msg, producer, consumer)
                consumer.commit()
        except kafka_errors.NoBrokersAvailable:
            time.sleep(2)
        except Exception:
            time.sleep(2)
        finally:
            try:
                if producer:
                    producer.flush(5)
                    producer.close()
            except Exception:
                pass
            try:
                if consumer:
                    consumer.close()
            except Exception:
                pass


@app.on_event("startup")
def startup():
    global worker_thread
    if worker_thread and worker_thread.is_alive():
        return
    worker_thread = threading.Thread(target=_run_worker, daemon=True)
    worker_thread.start()


@app.on_event("shutdown")
def shutdown():
    stop_event.set()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "consumer_id": state["consumer_id"],
        "processed": state["processed"],
        "retries": state["retries"],
        "dlq": state["dlq"],
        "failures": state["failures"],
        "last_backlog": state["last_backlog"],
        "last_message_at": state["last_message_at"],
    }
