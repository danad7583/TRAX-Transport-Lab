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
    "signed_envelope.create",
    "signed_envelope.verify",
    "checkpoint.create_signed_checkpoint",
    "checkpoint.verify_signed_checkpoint",
    "hash_bound.message",
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
    mode: str = "signed-envelope"
    events: list[MetricEvent] = field(default_factory=list)
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    frames_sent: int = 0
    frames_received: int = 0
    datagrams_sent: int = 0
    datagrams_received: int = 0
    payload_bytes: int = 0
    dag_nodes_appended: int = 0
    signed_envelope_create_count: int = 0
    signed_envelope_verify_count: int = 0
    signed_checkpoint_create_count: int = 0
    signed_checkpoint_verify_count: int = 0
    hash_bound_message_count: int = 0
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

    def add_signed_envelope_create(self) -> None:
        with self._lock:
            self.signed_envelope_create_count += 1

    def add_signed_envelope_verify(self) -> None:
        with self._lock:
            self.signed_envelope_verify_count += 1

    def add_signed_checkpoint_create(self) -> None:
        with self._lock:
            self.signed_checkpoint_create_count += 1

    def add_signed_checkpoint_verify(self) -> None:
        with self._lock:
            self.signed_checkpoint_verify_count += 1

    def add_hash_bound_message(self) -> None:
        with self._lock:
            self.hash_bound_message_count += 1

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
        return self.event_sum_summary()

    def wall_clock_summary(self) -> dict[str, float]:
        return {
            "total_wall_ms": self.total_duration_ns() / 1_000_000,
            "session_handshake_wall_ms": self.named_event_total_ms(
                "session_handshake_total"
            ),
            "stream_exchange_wall_ms": self.named_event_total_ms(
                "stream_exchange_total"
            ),
        }

    def event_sum_summary(self) -> dict[str, float]:
        return {
            "trax_primitives_event_ms": self.category_duration_ms(CATEGORY_TRAX),
            "python_packaging_event_ms": self.category_duration_ms(
                CATEGORY_PYTHON_PACKAGING
            ),
            "transport_io_event_ms": self.category_duration_ms(CATEGORY_TRANSPORT_IO),
            "dag_event_ms": self.category_duration_ms(CATEGORY_DAG),
            "orchestration_event_ms": self.category_duration_ms(CATEGORY_ORCHESTRATION),
            "unclassified_event_ms": self.category_duration_ms(CATEGORY_UNCLASSIFIED),
        }

    def micro_highlights(self) -> dict[str, float]:
        create_us = self.named_event_total_us("trax.create_admission_envelope_v1")
        verify_us = self.named_event_total_us("trax.verify_admission_envelope_v1_for_receiver")
        return {
            "payload_hash_verify_us": self.named_event_total_us("payload_hash_verify"),
            "trax_hash32_event_us": self.named_event_total_us("trax.hash32"),
            "trax_create_envelope_event_us": create_us,
            "trax_create_envelope_event_ms": create_us / 1_000,
            "trax_verify_envelope_event_us": verify_us,
            "trax_verify_envelope_event_ms": verify_us / 1_000,
            "signed_envelope_create_event_ms": self.named_event_total_ms(
                "signed_envelope.create"
            ),
            "signed_envelope_verify_event_ms": self.named_event_total_ms(
                "signed_envelope.verify"
            ),
            "signed_checkpoint_create_event_ms": self.named_event_total_ms(
                "checkpoint.create_signed_checkpoint"
            ),
            "signed_checkpoint_verify_event_ms": self.named_event_total_ms(
                "checkpoint.verify_signed_checkpoint"
            ),
            "hash_bound_messages_event_us": self.named_event_total_us("hash_bound.message"),
            "dag_append_event_us": self.named_event_total_us("dag.append_node"),
        }

    def key_event_summary(self) -> dict[str, float]:
        summary = {
            "session_handshake_wall_ms": self.named_event_total_ms("session_handshake_total"),
            "stream_exchange_wall_ms": self.named_event_total_ms("stream_exchange_total"),
            "payload_hash_verify_ms": self.named_event_total_ms("payload_hash_verify"),
            "payload_hash_verify_us": self.named_event_total_us("payload_hash_verify"),
            "create_envelope_event_ms": self.named_event_total_ms(
                "trax.create_admission_envelope_v1"
            ),
            "verify_envelope_event_ms": self.named_event_total_ms(
                "trax.verify_admission_envelope_v1_for_receiver"
            ),
            "message_encode_event_ms": self.named_event_total_ms("message.encode"),
            "message_decode_event_ms": self.named_event_total_ms("message.decode"),
            "transport_send_event_ms": self.named_event_total_ms("tcp.send_frame")
            + self.named_event_total_ms("udp.send_datagram"),
            "transport_receive_event_ms": self.named_event_total_ms("tcp.recv_frame")
            + self.named_event_total_ms("udp.recv_datagram"),
            "dag_append_event_ms": self.named_event_total_ms("dag.append_node"),
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

    def signing_counts_summary(self) -> dict[str, int]:
        return {
            "signed_envelope_create_count": self.signed_envelope_create_count,
            "signed_envelope_verify_count": self.signed_envelope_verify_count,
            "signed_checkpoint_create_count": self.signed_checkpoint_create_count,
            "signed_checkpoint_verify_count": self.signed_checkpoint_verify_count,
            "hash_bound_message_count": self.hash_bound_message_count,
        }

    def counts_summary(self) -> dict[str, int]:
        return {
            "bytes_sent": self.total_bytes_sent,
            "bytes_received": self.total_bytes_received,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "datagrams_sent": self.datagrams_sent,
            "datagrams_received": self.datagrams_received,
            "payload_bytes": self.payload_bytes,
            "dag_nodes_appended": self.dag_nodes_appended,
            **self.signing_counts_summary(),
        }

    def slowest_events(
        self,
        limit: int = 10,
        *,
        include_categories: set[str] | None = None,
    ) -> list[dict]:
        events = [
            event for event in self.events
            if include_categories is None or event.category in include_categories
        ]
        events = sorted(events, key=lambda event: max(0, event.duration_ns), reverse=True)
        return [
            {
                "name": event.name,
                "category": event.category,
                "duration_ms": max(0, event.duration_ns) / 1_000_000,
                "duration_us": max(0, event.duration_ns) / 1_000,
                "bytes_sent": event.bytes_sent,
                "bytes_received": event.bytes_received,
                "frames_sent": event.frames_sent,
                "frames_received": event.frames_received,
                "datagrams_sent": event.datagrams_sent,
                "datagrams_received": event.datagrams_received,
                "ok": event.ok,
                "detail": event.detail,
            }
            for event in events[:limit]
        ]

    def compact_dict(self) -> dict:
        return {
            "transport": self.transport,
            "mode": self.mode,
            "note": "local loopback diagnostic metrics; not benchmark-grade results",
            "wall_clock": self.wall_clock_summary(),
            "event_sums": self.event_sum_summary(),
            "micro_highlights": self.micro_highlights(),
            "counts": self.counts_summary(),
            "signing_counts": self.signing_counts_summary(),
            "final_tip": self.final_tip,
            "key_event_summary": self.key_event_summary(),
            "slowest_events": self.slowest_events(10),
        }

    def full_dict(self) -> dict:
        payload = self.compact_dict()
        payload.update({
            "events_by_category": {
                category: [event.as_dict() for event in events]
                for category, events in self.events_by_category().items()
            },
            "events": [event.as_dict() for event in self.events],
        })
        return payload

    def as_dict(self, include_events: bool = False) -> dict:
        return self.full_dict() if include_events else self.compact_dict()

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    def summary_lines(self) -> list[str]:
        wall_clock = self.wall_clock_summary()
        event_sums = self.event_sum_summary()
        micro = self.micro_highlights()
        counts = self.counts_summary()
        signing_counts = self.signing_counts_summary()
        lines = [
            "Mode:",
            self.mode,
            "",
            "Wall-clock:",
            f"total_wall_ms: {wall_clock['total_wall_ms']:.3f}",
            f"session_handshake_wall_ms: {wall_clock['session_handshake_wall_ms']:.3f}",
            f"stream_exchange_wall_ms: {wall_clock['stream_exchange_wall_ms']:.3f}",
            "",
            "Event sums, may overlap:",
            f"trax_primitives_event_ms: {event_sums['trax_primitives_event_ms']:.3f}",
            f"python_packaging_event_ms: {event_sums['python_packaging_event_ms']:.3f}",
            f"transport_io_event_ms: {event_sums['transport_io_event_ms']:.3f}",
            f"dag_event_ms: {event_sums['dag_event_ms']:.3f}",
            f"orchestration_event_ms: {event_sums['orchestration_event_ms']:.3f}",
            f"unclassified_event_ms: {event_sums['unclassified_event_ms']:.3f}",
            "",
            "Primitive highlights:",
            f"payload_hash_verify_us: {micro['payload_hash_verify_us']:.3f}",
            f"dag_append_event_us: {micro['dag_append_event_us']:.3f}",
            f"trax_hash32_event_us: {micro['trax_hash32_event_us']:.3f}",
            f"trax_create_envelope_event_us: {micro['trax_create_envelope_event_us']:.3f}",
            f"trax_create_envelope_event_ms: {micro['trax_create_envelope_event_ms']:.3f}",
            f"trax_verify_envelope_event_us: {micro['trax_verify_envelope_event_us']:.3f}",
            f"trax_verify_envelope_event_ms: {micro['trax_verify_envelope_event_ms']:.3f}",
            f"signed_envelope_create_event_ms: {micro['signed_envelope_create_event_ms']:.3f}",
            f"signed_envelope_verify_event_ms: {micro['signed_envelope_verify_event_ms']:.3f}",
            f"signed_checkpoint_create_event_ms: {micro['signed_checkpoint_create_event_ms']:.3f}",
            f"signed_checkpoint_verify_event_ms: {micro['signed_checkpoint_verify_event_ms']:.3f}",
            f"hash_bound_messages_event_us: {micro['hash_bound_messages_event_us']:.3f}",
            "",
            "Counts:",
            f"bytes_sent: {counts['bytes_sent']}",
            f"bytes_received: {counts['bytes_received']}",
        ]
        if self.transport == "tcp":
            lines.extend(
                [
                    f"frames_sent: {counts['frames_sent']}",
                    f"frames_received: {counts['frames_received']}",
                ]
            )
        if self.transport == "udp":
            lines.extend(
                [
                    f"datagrams_sent: {counts['datagrams_sent']}",
                    f"datagrams_received: {counts['datagrams_received']}",
                ]
            )
        lines.extend(
            [
                f"payload_bytes: {counts['payload_bytes']}",
                f"dag_nodes_appended: {counts['dag_nodes_appended']}",
                "",
                "Signing counts:",
                f"signed_envelope_create_count: {signing_counts['signed_envelope_create_count']}",
                f"signed_envelope_verify_count: {signing_counts['signed_envelope_verify_count']}",
                f"signed_checkpoint_create_count: {signing_counts['signed_checkpoint_create_count']}",
                f"signed_checkpoint_verify_count: {signing_counts['signed_checkpoint_verify_count']}",
                f"hash_bound_message_count: {signing_counts['hash_bound_message_count']}",
                "",
                "Delta-ready metrics:",
                f"payload_hash_verify_us: {micro['payload_hash_verify_us']:.3f}",
                f"dag_append_event_us: {micro['dag_append_event_us']:.3f}",
                f"signed_envelope_create_event_ms: {micro['signed_envelope_create_event_ms']:.3f}",
                f"signed_envelope_verify_event_ms: {micro['signed_envelope_verify_event_ms']:.3f}",
                f"signed_checkpoint_create_event_ms: {micro['signed_checkpoint_create_event_ms']:.3f}",
                f"signed_checkpoint_verify_event_ms: {micro['signed_checkpoint_verify_event_ms']:.3f}",
                "",
                "Slowest events:",
            ]
        )
        for index, event in enumerate(self.slowest_events(10), start=1):
            lines.append(
                f"{index}. {event['name']} [{event['category']}] {event['duration_ms']:.3f} ms"
            )
        lines.extend([
            "",
            "Interpretation:",
            "local loopback diagnostic metrics; not benchmark-grade results",
            "wall-clock values measure elapsed demo time",
            "event-sum values may overlap across client/server threads and nested operations",
        ])
        return lines


def summarize_metric_runs(metrics_runs: list[RunMetrics]) -> dict:
    if not metrics_runs:
        raise ValueError("metrics_runs must not be empty")

    fields = {
        "total_wall_ms": lambda m: m.wall_clock_summary()["total_wall_ms"],
        "session_handshake_wall_ms": lambda m: m.wall_clock_summary()[
            "session_handshake_wall_ms"
        ],
        "stream_exchange_wall_ms": lambda m: m.wall_clock_summary()[
            "stream_exchange_wall_ms"
        ],
        "payload_hash_verify_us": lambda m: m.micro_highlights()[
            "payload_hash_verify_us"
        ],
        "dag_append_event_us": lambda m: m.micro_highlights()["dag_append_event_us"],
        "hash_bound_messages_event_us": lambda m: m.micro_highlights()[
            "hash_bound_messages_event_us"
        ],
        "signed_envelope_create_event_ms": lambda m: m.micro_highlights()[
            "signed_envelope_create_event_ms"
        ],
        "signed_envelope_verify_event_ms": lambda m: m.micro_highlights()[
            "signed_envelope_verify_event_ms"
        ],
        "signed_checkpoint_create_event_ms": lambda m: m.micro_highlights()[
            "signed_checkpoint_create_event_ms"
        ],
        "signed_checkpoint_verify_event_ms": lambda m: m.micro_highlights()[
            "signed_checkpoint_verify_event_ms"
        ],
        "signed_envelope_create_count": lambda m: m.signing_counts_summary()[
            "signed_envelope_create_count"
        ],
        "signed_envelope_verify_count": lambda m: m.signing_counts_summary()[
            "signed_envelope_verify_count"
        ],
        "signed_checkpoint_create_count": lambda m: m.signing_counts_summary()[
            "signed_checkpoint_create_count"
        ],
        "signed_checkpoint_verify_count": lambda m: m.signing_counts_summary()[
            "signed_checkpoint_verify_count"
        ],
        "hash_bound_message_count": lambda m: m.signing_counts_summary()[
            "hash_bound_message_count"
        ],
        "trax_hash32_event_us": lambda m: m.micro_highlights()["trax_hash32_event_us"],
        "trax_create_envelope_event_ms": lambda m: m.micro_highlights()[
            "trax_create_envelope_event_ms"
        ],
        "trax_verify_envelope_event_ms": lambda m: m.micro_highlights()[
            "trax_verify_envelope_event_ms"
        ],
        "trax_primitives_event_ms": lambda m: m.event_sum_summary()[
            "trax_primitives_event_ms"
        ],
        "python_packaging_event_ms": lambda m: m.event_sum_summary()[
            "python_packaging_event_ms"
        ],
        "transport_io_event_ms": lambda m: m.event_sum_summary()[
            "transport_io_event_ms"
        ],
        "dag_event_ms": lambda m: m.event_sum_summary()["dag_event_ms"],
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
        "",
        "Wall-clock:",
    ]
    for name in [
        "total_wall_ms",
        "session_handshake_wall_ms",
        "stream_exchange_wall_ms",
    ]:
        values = summary[name]
        lines.append(
            f"{name}: min={values['min']:.3f} avg={values['avg']:.3f} max={values['max']:.3f}"
        )
    lines.extend(["", "Event sums, may overlap:"])
    for name in [
        "trax_primitives_event_ms",
        "python_packaging_event_ms",
        "transport_io_event_ms",
        "dag_event_ms",
    ]:
        values = summary[name]
        lines.append(
            f"{name}: min={values['min']:.3f} avg={values['avg']:.3f} max={values['max']:.3f}"
        )
    lines.extend(["", "Primitive highlights:"])
    for name in [
        "payload_hash_verify_us",
        "dag_append_event_us",
        "hash_bound_messages_event_us",
        "trax_hash32_event_us",
        "trax_create_envelope_event_ms",
        "trax_verify_envelope_event_ms",
        "signed_envelope_create_event_ms",
        "signed_envelope_verify_event_ms",
        "signed_checkpoint_create_event_ms",
        "signed_checkpoint_verify_event_ms",
    ]:
        values = summary[name]
        lines.append(
            f"{name}: min={values['min']:.3f} avg={values['avg']:.3f} max={values['max']:.3f}"
        )
    return lines
