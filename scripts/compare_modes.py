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
from trax_transport_lab.tcp_demo import CHECKPOINT_MODE, SIGNED_ENVELOPE_MODE, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


TRANSPORTS = ("tcp", "udp")


def _run_transport(transport: str, mode: str, runs: int):
    fn = run_tcp_demo if transport == "tcp" else run_udp_demo
    return [fn(mode=mode) for _ in range(runs)]


def _avg(summary: dict, name: str) -> float:
    return summary[name]["avg"]


def _signing_operation_count(metrics: RunMetrics) -> int:
    counts = metrics.signing_counts_summary()
    return (
        counts["signed_envelope_create_count"]
        + counts["signed_envelope_verify_count"]
        + counts["signed_checkpoint_create_count"]
        + counts["signed_checkpoint_verify_count"]
    )


def _operation_count_summary(metrics_runs: list[RunMetrics]) -> dict[str, float]:
    values = [_signing_operation_count(metrics) for metrics in metrics_runs]
    return {
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _mode_payload(transport: str, mode: str, runs: int) -> dict:
    results = _run_transport(transport, mode, runs)
    metrics_runs = [result.metrics for result in results]
    summary = summarize_metric_runs(metrics_runs)
    summary["signing_operation_count"] = _operation_count_summary(metrics_runs)
    return {
        "ok": all(result.ok for result in results),
        "transport": transport,
        "mode": mode,
        "runs": runs,
        "summary": summary,
        "final_tips": [
            result.final_tip.hex() if result.final_tip else None
            for result in results
        ],
    }


def _delta_payload(signed_payload: dict, checkpoint_payload: dict) -> dict:
    signed = signed_payload["summary"]
    checkpoint = checkpoint_payload["summary"]
    signed_wall = _avg(signed, "total_wall_ms")
    checkpoint_wall = _avg(checkpoint, "total_wall_ms")
    wall_delta = checkpoint_wall - signed_wall
    if signed_wall:
        wall_delta_percent = (wall_delta / signed_wall) * 100
    else:
        wall_delta_percent = 0.0
    return {
        "total_wall_ms_delta": wall_delta,
        "total_wall_ms_delta_percent": wall_delta_percent,
        "signed_create_event_ms_delta": _avg(
            checkpoint, "signed_envelope_create_event_ms"
        )
        - _avg(signed, "signed_envelope_create_event_ms"),
        "signed_verify_event_ms_delta": _avg(
            checkpoint, "signed_envelope_verify_event_ms"
        )
        - _avg(signed, "signed_envelope_verify_event_ms"),
        "signing_operation_count_delta": _avg(
            checkpoint, "signing_operation_count"
        )
        - _avg(signed, "signing_operation_count"),
    }


def comparison_payload(transports: list[str], runs: int) -> dict:
    payload = {
        "note": "local loopback diagnostic metrics; not benchmark-grade results",
        "runs": runs,
        "transports": {},
    }
    for transport in transports:
        signed = _mode_payload(transport, SIGNED_ENVELOPE_MODE, runs)
        checkpoint = _mode_payload(transport, CHECKPOINT_MODE, runs)
        payload["transports"][transport] = {
            SIGNED_ENVELOPE_MODE: signed,
            CHECKPOINT_MODE: checkpoint,
            "delta": _delta_payload(signed, checkpoint),
            "ok": signed["ok"] and checkpoint["ok"],
        }
    payload["ok"] = all(item["ok"] for item in payload["transports"].values())
    payload["interpretation"] = (
        "Checkpoint mode reduces per-message signing work by replacing intermediate "
        "signed envelopes with hash-bound continuity and a signed checkpoint in this "
        "local diagnostic run."
    )
    return payload


def _print_mode_summary(label: str, summary: dict) -> None:
    print(f"{label}:")
    for name in [
        "total_wall_ms",
        "signed_envelope_create_count",
        "signed_envelope_verify_count",
        "signed_checkpoint_create_count",
        "signed_checkpoint_verify_count",
        "hash_bound_message_count",
        "signed_envelope_create_event_ms",
        "signed_envelope_verify_event_ms",
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
        signed = transport_payload[SIGNED_ENVELOPE_MODE]
        checkpoint = transport_payload[CHECKPOINT_MODE]
        print(f"transport: {transport}")
        print(f"runs: {payload['runs']}")
        print(f"ok: {transport_payload['ok']}")
        print()
        _print_mode_summary("Signed-envelope mode", signed["summary"])
        print()
        _print_mode_summary("Checkpoint mode", checkpoint["summary"])
        print()
        print("Delta:")
        for name, value in transport_payload["delta"].items():
            print(f"  {name}: {value:.3f}")
        print()
    print("Interpretation:")
    print(payload["interpretation"])
    print("These are local loopback diagnostic metrics, not benchmark-grade claims.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare signed-envelope and checkpoint modes.")
    parser.add_argument("--runs", type=int, default=5, help="number of runs per mode")
    parser.add_argument(
        "--transport",
        choices=("both", "tcp", "udp"),
        default="both",
        help="transport to compare",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    args = parser.parse_args(argv)

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    transports = list(TRANSPORTS if args.transport == "both" else (args.transport,))
    payload = comparison_payload(transports, args.runs)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print_text(payload)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
