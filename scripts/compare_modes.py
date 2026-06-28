from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trax_transport_lab.metrics import RunMetrics, summarize_metric_runs
from trax_transport_lab.scaled import (
    ScaleConfig,
    add_scale_arguments,
    scale_config_from_args,
)
from trax_transport_lab.tcp_demo import (
    CHECKPOINT_MODE,
    DAG_GENESIS_MODE,
    MODE_CHOICES,
    SIGNED_ENVELOPE_MODE,
    run_tcp_demo,
)
from trax_transport_lab.udp_demo import run_udp_demo


TRANSPORTS = ("tcp", "udp")


def _run_transport(
    transport: str,
    mode: str,
    runs: int,
    scale_config: ScaleConfig | None = None,
):
    fn = run_tcp_demo if transport == "tcp" else run_udp_demo
    return [fn(mode=mode, scale_config=scale_config) for _ in range(runs)]


def _avg(summary: dict, name: str) -> float:
    return summary[name]["avg"]


def _signing_operation_count(metrics: RunMetrics) -> int:
    counts = metrics.signing_counts_summary()
    return (
        counts["signed_envelope_create_count"]
        + counts["signed_envelope_verify_count"]
        + counts["signed_checkpoint_create_count"]
        + counts["signed_checkpoint_verify_count"]
        + counts["signed_genesis_create_count"]
        + counts["signed_genesis_verify_count"]
    )


def _operation_count_summary(metrics_runs: list[RunMetrics]) -> dict[str, float]:
    values = [_signing_operation_count(metrics) for metrics in metrics_runs]
    return {
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _mode_payload(
    transport: str,
    mode: str,
    runs: int,
    scale_config: ScaleConfig | None = None,
) -> dict:
    results = _run_transport(transport, mode, runs, scale_config)
    metrics_runs = [result.metrics for result in results]
    summary = summarize_metric_runs(metrics_runs)
    summary["signing_operation_count"] = _operation_count_summary(metrics_runs)
    return {
        "ok": all(result.ok for result in results),
        "transport": transport,
        "mode": mode,
        "runs": runs,
        "messages": scale_config.messages if scale_config else None,
        "summary": summary,
        "final_tips": [
            result.final_tip.hex() if result.final_tip else None
            for result in results
        ],
    }


def _delta_payload(mode_a_payload: dict, mode_b_payload: dict) -> dict:
    mode_a = mode_a_payload["summary"]
    mode_b = mode_b_payload["summary"]
    mode_a_wall = _avg(mode_a, "total_wall_ms")
    mode_b_wall = _avg(mode_b, "total_wall_ms")
    wall_delta = mode_b_wall - mode_a_wall
    if mode_a_wall:
        wall_delta_percent = (wall_delta / mode_a_wall) * 100
    else:
        wall_delta_percent = 0.0
    return {
        "hot_path_signed_packet_count_delta": _avg(
            mode_b, "hot_path_signed_packet_count"
        )
        - _avg(mode_a, "hot_path_signed_packet_count"),
        "avg_message_wall_us_delta": _avg(mode_b, "avg_message_wall_us")
        - _avg(mode_a, "avg_message_wall_us"),
        "messages_per_second_delta": _avg(mode_b, "messages_per_second")
        - _avg(mode_a, "messages_per_second"),
        "signed_envelope_event_ms_delta": _avg(
            mode_b, "signed_envelope_create_event_ms"
        )
        + _avg(mode_b, "signed_envelope_verify_event_ms")
        - _avg(mode_a, "signed_envelope_create_event_ms")
        - _avg(mode_a, "signed_envelope_verify_event_ms"),
        "total_wall_ms_delta": wall_delta,
        "total_wall_ms_delta_percent": wall_delta_percent,
        "signed_create_event_ms_delta": _avg(
            mode_b, "signed_envelope_create_event_ms"
        )
        - _avg(mode_a, "signed_envelope_create_event_ms"),
        "signed_verify_event_ms_delta": _avg(
            mode_b, "signed_envelope_verify_event_ms"
        )
        - _avg(mode_a, "signed_envelope_verify_event_ms"),
        "signing_operation_count_delta": _avg(
            mode_b, "signing_operation_count"
        )
        - _avg(mode_a, "signing_operation_count"),
        "dag_event_ms_delta": _avg(mode_b, "dag_event_ms")
        - _avg(mode_a, "dag_event_ms"),
        "dag_segment_count_delta": _avg(mode_b, "dag_segment_count")
        - _avg(mode_a, "dag_segment_count"),
        "agent_key_rotation_event_count_delta": _avg(
            mode_b, "agent_key_rotation_event_count"
        )
        - _avg(mode_a, "agent_key_rotation_event_count"),
        "dag_key_rotation_event_count_delta": _avg(
            mode_b, "dag_key_rotation_event_count"
        )
        - _avg(mode_a, "dag_key_rotation_event_count"),
    }


def comparison_payload(
    transports: list[str],
    runs: int,
    mode_a: str = SIGNED_ENVELOPE_MODE,
    mode_b: str = DAG_GENESIS_MODE,
    scale_config: ScaleConfig | None = None,
) -> dict:
    payload = {
        "note": "local loopback diagnostic metrics; not benchmark-grade results",
        "runs": runs,
        "mode_a": mode_a,
        "mode_b": mode_b,
        "messages": scale_config.messages if scale_config else None,
        "transports": {},
    }
    for transport in transports:
        first = _mode_payload(transport, mode_a, runs, scale_config)
        second = _mode_payload(transport, mode_b, runs, scale_config)
        payload["transports"][transport] = {
            mode_a: first,
            mode_b: second,
            "delta": _delta_payload(first, second),
            "ok": first["ok"] and second["ok"],
        }
    payload["ok"] = all(item["ok"] for item in payload["transports"].values())
    payload["interpretation"] = (
        "DAG-genesis mode removes per-message AAIP envelope signing from the hot path. "
        "The remaining security check is DAG continuity from signed genesis in this "
        "local diagnostic run."
    )
    return payload


def _print_mode_summary(label: str, summary: dict) -> None:
    print(f"{label}:")
    for name in [
        "total_wall_ms",
        "hot_path_signed_packet_count",
        "signed_envelope_create_count",
        "signed_envelope_verify_count",
        "signed_genesis_create_count",
        "signed_genesis_verify_count",
        "signed_checkpoint_create_count",
        "signed_checkpoint_verify_count",
        "hash_bound_message_count",
        "post_genesis_wall_ms",
        "avg_message_wall_us",
        "messages_per_second",
        "dag_segment_count",
        "dag_segment_event_ms",
        "agent_key_rotation_event_count",
        "agent_key_rotation_signed_packet_count",
        "agent_key_rotation_event_ms",
        "dag_key_rotation_event_count",
        "dag_key_rotation_event_ms",
        "dag_nodes_retained",
        "dag_nodes_pruned",
        "signed_envelope_create_event_ms",
        "signed_envelope_verify_event_ms",
        "signed_genesis_create_event_ms",
        "signed_genesis_verify_event_ms",
        "hot_path_signed_packet_event_ms",
        "signed_checkpoint_create_event_ms",
        "signed_checkpoint_verify_event_ms",
        "payload_hash_verify_us",
        "dag_append_event_us",
    ]:
        if name in summary:
            print(f"  avg {name}: {_avg(summary, name):.3f}")


def print_text(payload: dict) -> None:
    print("TRAX mode comparison")
    print("local loopback diagnostic metrics; not benchmark-grade results")
    print()
    for transport, transport_payload in payload["transports"].items():
        first = transport_payload[payload["mode_a"]]
        second = transport_payload[payload["mode_b"]]
        print(f"transport: {transport}")
        print(f"runs: {payload['runs']}")
        print(f"ok: {transport_payload['ok']}")
        print()
        _print_mode_summary(f"{payload['mode_a']} mode", first["summary"])
        print()
        _print_mode_summary(f"{payload['mode_b']} mode", second["summary"])
        print()
        print("Delta:")
        for name, value in transport_payload["delta"].items():
            print(f"  {name}: {value:.3f}")
        print()
    print("Interpretation:")
    print(payload["interpretation"])
    print("These are local loopback diagnostic metrics, not benchmark-grade claims.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare TRAX transport lab modes.")
    parser.add_argument("--runs", type=int, default=5, help="number of runs per mode")
    parser.add_argument("--mode-a", choices=MODE_CHOICES, default=SIGNED_ENVELOPE_MODE)
    parser.add_argument("--mode-b", choices=MODE_CHOICES, default=DAG_GENESIS_MODE)
    parser.add_argument(
        "--transport",
        choices=("both", "tcp", "udp"),
        default="both",
        help="transport to compare",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    add_scale_arguments(parser)
    args = parser.parse_args(argv)

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")
    try:
        scale_config = scale_config_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))

    transports = list(TRANSPORTS if args.transport == "both" else (args.transport,))
    payload = comparison_payload(
        transports,
        args.runs,
        args.mode_a,
        args.mode_b,
        scale_config,
    )
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print_text(payload)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
