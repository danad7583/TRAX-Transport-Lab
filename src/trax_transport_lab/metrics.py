from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import threading
from time import perf_counter_ns
from typing import Iterator


@dataclass
class MetricEvent:
    name: str
    started_ns: int
    ended_ns: int
    duration_ns: int
    bytes_sent: int = 0
    bytes_received: int = 0
    ok: bool = True
    detail: str = ""

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "started_ns": self.started_ns,
            "ended_ns": self.ended_ns,
            "duration_ns": self.duration_ns,
            "duration_ms": self.duration_ns / 1_000_000,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "ok": self.ok,
            "detail": self.detail,
        }


@dataclass
class _Measurement:
    metrics: "RunMetrics"
    name: str
    started_ns: int = field(default_factory=perf_counter_ns)
    bytes_sent: int = 0
    bytes_received: int = 0
    ok: bool = True
    detail: str = ""

    def add_sent(self, byte_count: int) -> None:
        self.metrics.add_bytes_sent(byte_count)

    def add_received(self, byte_count: int) -> None:
        self.metrics.add_bytes_received(byte_count)


@dataclass
class RunMetrics:
    transport: str
    events: list[MetricEvent] = field(default_factory=list)
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    datagrams_sent: int = 0
    datagrams_received: int = 0
    payload_bytes: int = 0
    dag_nodes_appended: int = 0
    final_tip: str | None = None
    started_ns: int = field(default_factory=perf_counter_ns)
    ended_ns: int | None = None

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._local = threading.local()

    @contextmanager
    def measure(self, name: str) -> Iterator[_Measurement]:
        measurement = _Measurement(self, name)
        stack = getattr(self._local, "stack", [])
        stack.append(measurement)
        self._local.stack = stack
        try:
            yield measurement
        except Exception as exc:
            measurement.ok = False
            measurement.detail = str(exc)
            raise
        finally:
            ended_ns = perf_counter_ns()
            self.add_event(
                MetricEvent(
                    name=name,
                    started_ns=measurement.started_ns,
                    ended_ns=ended_ns,
                    duration_ns=ended_ns - measurement.started_ns,
                    bytes_sent=measurement.bytes_sent,
                    bytes_received=measurement.bytes_received,
                    ok=measurement.ok,
                    detail=measurement.detail,
                )
            )
            stack.pop()

    def add_event(self, event: MetricEvent) -> None:
        with self._lock:
            self.events.append(event)

    def add_bytes_sent(self, byte_count: int) -> None:
        with self._lock:
            self.total_bytes_sent += byte_count
            stack = getattr(self._local, "stack", [])
            for measurement in stack:
                measurement.bytes_sent += byte_count

    def add_bytes_received(self, byte_count: int) -> None:
        with self._lock:
            self.total_bytes_received += byte_count
            stack = getattr(self._local, "stack", [])
            for measurement in stack:
                measurement.bytes_received += byte_count

    def add_frame_sent(self) -> None:
        with self._lock:
            self.frames_sent += 1

    def add_frame_received(self) -> None:
        with self._lock:
            self.frames_received += 1

    def add_datagram_sent(self) -> None:
        with self._lock:
            self.datagrams_sent += 1

    def add_datagram_received(self) -> None:
        with self._lock:
            self.datagrams_received += 1

    def add_dag_node(self, final_tip: bytes | None = None) -> None:
        with self._lock:
            self.dag_nodes_appended += 1
            if final_tip is not None:
                self.final_tip = final_tip.hex()

    def set_payload_bytes(self, byte_count: int) -> None:
        with self._lock:
            self.payload_bytes = byte_count

    def finish(self, final_tip: bytes | None = None) -> None:
        with self._lock:
            self.ended_ns = perf_counter_ns()
            self.final_tip = final_tip.hex() if final_tip else self.final_tip

    def total_duration_ns(self) -> int:
        ended_ns = self.ended_ns if self.ended_ns is not None else perf_counter_ns()
        return max(0, ended_ns - self.started_ns)

    def duration_ms_for(self, event_name: str) -> float:
        return sum(
            event.duration_ns for event in self.events if event.name == event_name
        ) / 1_000_000

    def event_names(self) -> list[str]:
        return [event.name for event in self.events]

    def as_dict(self) -> dict:
        return {
            "transport": self.transport,
            "total_duration_ns": self.total_duration_ns(),
            "total_duration_ms": self.total_duration_ns() / 1_000_000,
            "total_bytes_sent": self.total_bytes_sent,
            "total_bytes_received": self.total_bytes_received,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "datagrams_sent": self.datagrams_sent,
            "datagrams_received": self.datagrams_received,
            "payload_bytes": self.payload_bytes,
            "dag_nodes_appended": self.dag_nodes_appended,
            "final_tip": self.final_tip,
            "events": [event.as_dict() for event in self.events],
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    def summary_lines(self) -> list[str]:
        lines = [
            "Metrics:",
            f"transport: {self.transport}",
            f"total_duration_ms: {self.total_duration_ns() / 1_000_000:.3f}",
            f"bytes_sent: {self.total_bytes_sent}",
            f"bytes_received: {self.total_bytes_received}",
        ]
        if self.transport == "tcp":
            lines.extend(
                [
                    f"frames_sent: {self.frames_sent}",
                    f"frames_received: {self.frames_received}",
                ]
            )
        if self.transport == "udp":
            lines.extend(
                [
                    f"datagrams_sent: {self.datagrams_sent}",
                    f"datagrams_received: {self.datagrams_received}",
                ]
            )
        lines.extend(
            [
                f"payload_bytes: {self.payload_bytes}",
                f"dag_nodes_appended: {self.dag_nodes_appended}",
                f"final_tip: {self.final_tip or '<none>'}",
                f"session_handshake_ms: {self.duration_ms_for('session_handshake_total'):.3f}",
                f"stream_exchange_ms: {self.duration_ms_for('stream_exchange_total'):.3f}",
                f"payload_hash_verify_ms: {self.duration_ms_for('payload_hash_verify'):.3f}",
            ]
        )
        return lines
