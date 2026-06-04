from __future__ import annotations

from typing import Any, Literal, Optional
from uuid import uuid4
import time

from pydantic import BaseModel, Field


ZoneId = Literal["Z1", "Z2", "Z3", "Z4", "Z5"]


class QueryRequest(BaseModel):
    qtype: Literal["Q1", "Q2", "Q3", "Q4", "Q5"]
    zone_id: Optional[ZoneId] = None
    zone_id_a: Optional[ZoneId] = None
    zone_id_b: Optional[ZoneId] = None
    confidence_min: float = Field(default=0.0, ge=0.0, le=1.0)
    bins: int = Field(default=5, ge=2, le=50)
    ttl_seconds: Optional[int] = Field(default=None, ge=1, le=86400)


class QueryResult(BaseModel):
    cache_key: str
    qtype: str
    zone_id: Optional[str] = None
    zone_id_a: Optional[str] = None
    zone_id_b: Optional[str] = None
    cache_hit: bool
    ttl_seconds: int
    response_ms: float
    compute_ms: float
    payload: Any


class KafkaQueryMessage(BaseModel):
    request_id: str = Field(default_factory=lambda: uuid4().hex)
    created_at: float = Field(default_factory=time.time)
    available_at: float = Field(default_factory=time.time)
    attempts: int = 0
    origin_topic: str = "queries.main"

    qtype: Literal["Q1", "Q2", "Q3", "Q4", "Q5"]
    zone_id: Optional[ZoneId] = None
    zone_id_a: Optional[ZoneId] = None
    zone_id_b: Optional[ZoneId] = None
    confidence_min: float = Field(default=0.0, ge=0.0, le=1.0)
    bins: int = Field(default=5, ge=2, le=50)
    ttl_seconds: Optional[int] = Field(default=None, ge=1, le=86400)

    def to_query_request(self) -> QueryRequest:
        return QueryRequest(
            qtype=self.qtype,
            zone_id=self.zone_id,
            zone_id_a=self.zone_id_a,
            zone_id_b=self.zone_id_b,
            confidence_min=self.confidence_min,
            bins=self.bins,
            ttl_seconds=self.ttl_seconds,
        )


class TelemetryEvent(BaseModel):
    timestamp: float = Field(default_factory=time.time)
    request_id: str
    event_type: Literal["generated", "processed", "retry", "dlq", "failure", "recovered"]
    qtype: str
    topic: str
    consumer_id: str = "producer"
    attempt: int = 0
    cache_hit: Optional[bool] = None
    latency_ms: float = 0.0
    compute_ms: float = 0.0
    backlog_size: int = 0
    retry_delay_ms: int = 0
    response_status: str = "ok"
    created_at: Optional[float] = None
    completed_at: Optional[float] = None
    extra: dict[str, Any] = Field(default_factory=dict)


class MetricsSummary(BaseModel):
    count: int = 0
    generated: int = 0
    processed: int = 0
    recovered: int = 0
    retries: int = 0
    failures: int = 0
    dlq: int = 0
    hit_rate: float = 0.0
    retry_rate: float = 0.0
    recovery_rate: float = 0.0
    dlq_rate: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    throughput_qps: float = 0.0
    backlog_size: int = 0
    recovery_time_ms: float = 0.0
    avg_attempts_per_processed: float = 0.0
