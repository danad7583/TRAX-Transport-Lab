from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import threading
from time import perf_counter_ns
from typing import Iterator


CATEGORY_TRAX = "trax_primitives"
CATEGORY_PYTHON_PACKAGING = "python_packaging"
CATEGORY_TRANSPORT_IO = "transport_io"
CATEGORY_DAG = "dag"
CATEGORY_ORCHESTRATION = "orchestration"
CATEGORY_UNCLASSIFIED = "unclassified"

CATEGORIES = [
    CATEGORY_TRAX,
    CATEGORY_PYTHON_PACKAGING,
    CATEGORY_TRANSPORT_IO,
    CATEGORY_DAG,
    CATEGORY_ORCHESTRATION,
    CATEGORY_UNCLASSIFIED,
]

KEY_EVENT_NAMES = [
    "session_handshake_total",
    "stream_exchange_total",
    "payload_hash_verify",
    "trax.create_admission_envelope_v1",
    "trax.verify_admission_envelope_v1_for_receiver",
    "message.encode",
    "message.decode",
    "tcp.send_frame",
    "tcp.recv_frame",
    "udp.send_datagram",
    "udp.recv_datagram",
    "dag.append_node",
]


@dataclass
class MetricEvent:
    category: str
    name: str
    started_ns: int
    ended_ns: int
    duration_ns: int
    bytes_sent: int = 0
    bytes_received: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    datagrams_sent: int = 0
    datagrams_received: int = 0
    ok: bool = True
    detail: str = ""

    def as_dict(self) -> dict:
        return {
            "category": self.category,
            "name": self.name,
            "started_ns": self.started_ns,
            "ended_ns": self.ended_ns,
            "duration_ns": max(0, self.duration_ns),
            "duration_ms": max(0, self.duration_ns) / 1_000_000,
            "duration_us": max(0, self.duration_ns) / 1_000,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "datagrams_sent": self.datagrams_sent,
            "datagrams_received": self.datagrams_received,
            "ok": self.ok,
            "detail": self.detail,
        }


@dataclass
class _Measurement:
    metrics: "RunMetrics"
    name: str
    category: str = CATEGORY_UNCLASSIFIED
    started_ns: int = field(default_factory=perf_counter_ns)
    bytes_sent: int = 0
    bytes_received: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    datagrams_sent: int = 0
    datagrams_received: int = 0
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
    def measure(
        self,
        name: str,
        category: str = CATEGORY_UNCLASSIFIED,
    ) -> Iterator[_Measurement]:
        measurement = _Measurement(self, name=name, category=category)
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
                    category=category,
                    name=name,
                    started_ns=measurement.started_ns,
                    ended_ns=ended_ns,
                    duration_ns=max(0, ended_ns - measurement.started_ns),
                    bytes_sent=measurement.bytes_sent,
                    bytes_received=measurement.bytes_received,
                    frames_sent=measurement.frames_sent,
                    frames_received=measurement.frames_received,
                    datagrams_sent=measurement.datagrams_sent,
                    datagrams_received=measurement.datagrams_received,
                    ok=measurement.ok,
                    detail=measurement.detail,
                )
            )
            stack.pop()

    def add_event(self, event: MetricEvent) -> None:
        with self._lock:
            self.events.append(event)

    def record_event(
        self,
        name: str,
        category: str,
        started_ns: int,
        ended_ns: int,
        ok: bool = True,
        detail: str = "",
    ) -> None:
        self.add_event(
            MetricEvent(
                category=category,
                name=name,
                started_ns=started_ns,
                ended_ns=ended_ns,
                duration_ns=max(0, ended_ns - started_ns),
                ok=ok,
                detail=detail,
            )
        )

    def _active_measurements(self) -> list[_Measurement]:
        return getattr(self._local, "stack", [])

    def add_bytes_sent(self, byte_count: int) -> None:
        with self._lock:
            self.total_bytes_sent += byte_count
            for measurement in self._active_measurements():
                measurement.bytes_sent += byte_count

    def add_bytes_received(self, byte_count: int) -> None:
        with self._lock:
            self.total_bytes_received += byte_count
            for measurement in self._active_measurements():
                measurement.bytes_received += byte_count

    def add_frame_sent(self) -> None:
        with self._lock:
            self.frames_sent += 1
            for measurement in self._active_measurements():
                measurement.frames_sent += 1

    def add_frame_received(self) -> None:
        with self._lock:
            self.frames_received += 1
            for measurement in self._active_measurements():
                measurement.frames_received += 1

    def add_datagram_sent(self) -> None:
        with self._lock:
            self.datagrams_sent += 1
            for measurement in self._active_measurements():
                measurement.datagrams_sent += 1

    def add_datagram_received(self) -> None:
        with self._lock:
            self.datagrams_received += 1
            for measurement in self._active_measurements():
                measurement.datagrams_received += 1

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

    def events_by_category(self) -> dict[str, list[MetricEvent]]:
        grouped = {category: [] for category in CATEGORIES}
        for event in self.events:
            grouped.setdefault(event.category, []).append(event)
        return grouped

    def category_duration_ns(self, category: str) -> int:
        return sum(
            max(0, event.duration_ns) for event in self.events if event.category == category
        )

    def category_duration_ms(self, category: str) -> float:
        return self.category_duration_ns(category) / 1_000_000

    def named_event_total_ns(self, name: str) -> int:
        return sum(max(0, event.duration_ns) for event in self.events if event.name == name)

    def named_event_total_ms(self, name: str) -> float:
        return self.named_event_total_ns(name) / 1_000_000

    def named_event_total_us(self, name: str) -> float:
        return self.named_event_total_ns(name) / 1_000

    def duration_ms_for(self, event_name: str) -> float:
        return self.named_event_total_ms(event_name)

    def event_names(self) -> list[str]:
        return [event.name for event in self.events]

    def bucket_summary(self) -> dict[str, float]:
        buckets = {
            f"{category}_ms": self.category_duration_ms(category)
            for category in CATEGORIES
        }
        classified_ns = sum(self.category_duration_ns(category) for category in CATEGORIES)
        buckets["unclassified_wall_ms"] = max(
            0, self.total_duration_ns() - classified_ns
        ) / 1_000_000
        return buckets

    def key_event_summary(self) -> dict[str, float]:
        summary = {
            "session_handshake_total_ms": self.named_event_total_ms("session_handshake_total"),
            "stream_exchange_total_ms": self.named_event_total_ms("stream_exchange_total"),
            "payload_hash_verify_ms": self.named_event_total_ms("payload_hash_verify"),
            "payload_hash_verify_us": self.named_event_total_us("payload_hash_verify"),
            "create_envelope_total_ms": self.named_event_total_ms(
                "trax.create_admission_envelope_v1"
            ),
            "verify_envelope_total_ms": self.named_event_total_ms(
                "trax.verify_admission_envelope_v1_for_receiver"
            ),
            "message_encode_total_ms": self.named_event_total_ms("message.encode"),
            "message_decode_total_ms": self.named_event_total_ms("message.decode"),
            "transport_send_total_ms": self.named_event_total_ms("tcp.send_frame")
            + self.named_event_total_ms("udp.send_datagram"),
            "transport_receive_total_ms": self.named_event_total_ms("tcp.recv_frame")
            + self.named_event_total_ms("udp.recv_datagram"),
            "dag_append_total_ms": self.named_event_total_ms("dag.append_node"),
            "trax.hash32_total_us": self.named_event_total_us("trax.hash32"),
            "trax.verify_admission_envelope_v1_for_receiver_total_us": self.named_event_total_us(
                "trax.verify_admission_envelope_v1_for_receiver"
            ),
            "trax.create_admission_envelope_v1_total_us": self.named_event_total_us(
                "trax.create_admission_envelope_v1"
            ),
            "dag.append_node_total_us": self.named_event_total_us("dag.append_node"),
        }
        return summary

    def as_dict(self) -> dict:
        return {
            "transport": self.transport,
            "total_duration_ns": self.total_duration_ns(),
            "total_duration_ms": self.total_duration_ns() / 1_000_000,
            "total_duration_us": self.total_duration_ns() / 1_000,
            "total_bytes_sent": self.total_bytes_sent,
            "total_bytes_received": self.total_bytes_received,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "datagrams_sent": self.datagrams_sent,
            "datagrams_received": self.datagrams_received,
            "payload_bytes": self.payload_bytes,
            "dag_nodes_appended": self.dag_nodes_appended,
            "final_tip": self.final_tip,
            "bucket_summary": self.bucket_summary(),
            "key_event_summary": self.key_event_summary(),
            "events_by_category": {
                category: [event.as_dict() for event in events]
                for category, events in self.events_by_category().items()
            },
            "events": [event.as_dict() for event in self.events],
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    def summary_lines(self) -> list[str]:
        buckets = self.bucket_summary()
        key_events = self.key_event_summary()
        lines = [
            "Metrics:",
            f"transport: {self.transport}",
            f"total_duration_ms: {self.total_duration_ns() / 1_000_000:.3f}",
            f"total_duration_us: {self.total_duration_ns() / 1_000:.3f}",
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
                "",
                "Buckets:",
                f"trax_primitives_ms: {buckets['trax_primitives_ms']:.3f}",
                f"python_packaging_ms: {buckets['python_packaging_ms']:.3f}",
                f"transport_io_ms: {buckets['transport_io_ms']:.3f}",
                f"dag_ms: {buckets['dag_ms']:.3f}",
                f"orchestration_ms: {buckets['orchestration_ms']:.3f}",
                f"unclassified_ms: {buckets['unclassified_wall_ms']:.3f}",
                "",
                "Key Events:",
                f"session_handshake_total_ms: {key_events['session_handshake_total_ms']:.3f}",
                f"stream_exchange_total_ms: {key_events['stream_exchange_total_ms']:.3f}",
                f"payload_hash_verify_ms: {key_events['payload_hash_verify_ms']:.3f}",
                f"payload_hash_verify_us: {key_events['payload_hash_verify_us']:.3f}",
                f"create_envelope_total_ms: {key_events['create_envelope_total_ms']:.3f}",
                f"verify_envelope_total_ms: {key_events['verify_envelope_total_ms']:.3f}",
                f"message_encode_total_ms: {key_events['message_encode_total_ms']:.3f}",
                f"message_decode_total_ms: {key_events['message_decode_total_ms']:.3f}",
                f"transport_send_total_ms: {key_events['transport_send_total_ms']:.3f}",
                f"transport_receive_total_ms: {key_events['transport_receive_total_ms']:.3f}",
                f"dag_append_total_ms: {key_events['dag_append_total_ms']:.3f}",
            ]
        )
        return lines


def summarize_metric_runs(metrics_runs: list[RunMetrics]) -> dict:
    if not metrics_runs:
        raise ValueError("metrics_runs must not be empty")

    fields = {
        "total_duration_ms": lambda m: m.total_duration_ns() / 1_000_000,
        "session_handshake_ms": lambda m: m.named_event_total_ms("session_handshake_total"),
        "stream_exchange_ms": lambda m: m.named_event_total_ms("stream_exchange_total"),
        "payload_hash_verify_us": lambda m: m.named_event_total_us("payload_hash_verify"),
        "trax_primitives_ms": lambda m: m.category_duration_ms(CATEGORY_TRAX),
        "python_packaging_ms": lambda m: m.category_duration_ms(CATEGORY_PYTHON_PACKAGING),
        "transport_io_ms": lambda m: m.category_duration_ms(CATEGORY_TRANSPORT_IO),
        "dag_ms": lambda m: m.category_duration_ms(CATEGORY_DAG),
    }

    summary: dict[str, dict[str, float]] = {}
    for name, getter in fields.items():
        values = [getter(metrics) for metrics in metrics_runs]
        summary[name] = {
            "min": min(values),
            "avg": sum(values) / len(values),
            "max": max(values),
        }
    return summary


def aggregate_summary_lines(transport: str, metrics_runs: list[RunMetrics]) -> list[str]:
    summary = summarize_metric_runs(metrics_runs)
    lines = [
        f"{transport.upper()} aggregate metrics ({len(metrics_runs)} runs):",
    ]
    for name, values in summary.items():
        lines.append(
            f"{name}: min={values['min']:.3f} avg={values['avg']:.3f} max={values['max']:.3f}"
        )
    return lines
