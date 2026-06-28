from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trax_transport_lab.metrics import summarize_metric_runs
from trax_transport_lab.scaled import (
    KEY_MODES,
    DEFAULT_DAG_SIGNING_CADENCE,
    DEFAULT_MAX_DAG_NODES,
    make_scale_config,
)
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, MODE_CHOICES, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


TRANSPORTS = ("tcp", "udp")
DEFAULT_COUNTS = [10, 100, 1000]


def _avg(summary: dict, name: str) -> float:
    return summary[name]["avg"]


def _run_many(transport: str, mode: str, runs: int, scale_config):
    fn = run_tcp_demo if transport == "tcp" else run_udp_demo
    return [fn(mode=mode, scale_config=scale_config) for _ in range(runs)]


def _count_payload(transport: str, mode: str, runs: int, count: int, args) -> dict:
    scale_config = make_scale_config(
        messages=count,
        dag_signing_cadence=args.dag_signing_cadence,
        agent_key_rotation_cadence=args.agent_key_rotation_cadence,
        key_rotation_cadence_alias_value=args.key_rotation_cadence,
        dag_key_rotation_cadence=args.dag_key_rotation_cadence,
        key_mode=args.key_mode,
        max_dag_nodes=args.max_dag_nodes,
        seal_final_partial=args.seal_final_partial,
    )
    results = _run_many(transport, mode, runs, scale_config)
    metrics_runs = [result.metrics for result in results]
    summary = summarize_metric_runs(metrics_runs)
    first_metrics = metrics_runs[0]
    return {
        "mode": mode,
        "transport": transport,
        "messages": count,
        "runs": runs,
        "ok": all(result.ok for result in results),
        "dag_signing_cadence": scale_config.dag_signing_cadence,
        "agent_key_rotation_cadence": scale_config.agent_key_rotation_cadence,
        "key_rotation_cadence_alias_value": scale_config.key_rotation_cadence_alias_value,
        "dag_key_rotation_cadence": scale_config.dag_key_rotation_cadence,
        "key_mode": scale_config.key_mode,
        "key_mode_simulated": first_metrics.key_mode_simulated,
        "max_dag_nodes": scale_config.max_dag_nodes,
        "seal_final_partial": scale_config.seal_final_partial,
        "summary": summary,
        "final_tips": [
            result.final_tip.hex() if result.final_tip else None
            for result in results
        ],
        "warnings": sorted({warning for metrics in metrics_runs for warning in metrics.warnings}),
    }


def comparison_payload(transports: list[str], mode: str, counts: list[int], runs: int, args) -> dict:
    payload = {
        "note": "local loopback diagnostic metrics; not benchmark-grade results",
        "mode": mode,
        "runs": runs,
        "transports": {},
    }
    for transport in transports:
        payload["transports"][transport] = {}
        for count in counts:
            payload["transports"][transport][str(count)] = _count_payload(
                transport,
                mode,
                runs,
                count,
                args,
            )
    payload["ok"] = all(
        count_payload["ok"]
        for transport_payload in payload["transports"].values()
        for count_payload in transport_payload.values()
    )
    return payload


def _print_count(payload: dict) -> None:
    summary = payload["summary"]
    print("TRAX scaled message comparison")
    print("local loopback diagnostic metrics; not benchmark-grade results")
    print()
    print(f"mode: {payload['mode']}")
    print(f"transport: {payload['transport']}")
    print(f"messages: {payload['messages']}")
    print(f"runs: {payload['runs']}")
    print(f"dag_signing_cadence: {payload['dag_signing_cadence']}")
    print(f"agent_key_rotation_cadence: {payload['agent_key_rotation_cadence']}")
    print(f"dag_key_rotation_cadence: {payload['dag_key_rotation_cadence']}")
    print(f"key_mode: {payload['key_mode']}")
    print(f"max_dag_nodes: {payload['max_dag_nodes']}")
    print(f"ok: {payload['ok']}")
    print()
    for name in [
        "total_wall_ms",
        "post_genesis_wall_ms",
        "avg_message_wall_us",
        "messages_per_second",
        "payload_hash_verify_us",
        "dag_append_event_us",
        "hash_bound_message_count",
        "hot_path_signed_packet_count",
        "signed_genesis_create_count",
        "signed_genesis_verify_count",
        "dag_segment_count",
        "dag_segment_event_ms",
        "agent_key_rotation_event_count",
        "agent_key_rotation_signed_packet_count",
        "agent_key_rotation_event_ms",
        "dag_key_rotation_event_count",
        "dag_key_rotation_event_ms",
        "dag_nodes_retained",
        "dag_nodes_pruned",
    ]:
        print(f"avg {name}: {_avg(summary, name):.3f}")
    if payload["warnings"]:
        print()
        print("Warnings:")
        for warning in payload["warnings"]:
            print(warning)
    print()


def print_text(payload: dict) -> None:
    for transport_payload in payload["transports"].values():
        for count_payload in transport_payload.values():
            _print_count(count_payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run scaled TRAX message-count comparisons.")
    parser.add_argument("--mode", choices=MODE_CHOICES, default=DAG_GENESIS_MODE)
    parser.add_argument("--transport", choices=("both", "tcp", "udp"), default="both")
    parser.add_argument("--counts", type=int, nargs="+", default=DEFAULT_COUNTS)
    parser.add_argument("--messages", type=int, default=None, help="single-count alias for --counts")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--dag-signing-cadence", type=int, default=DEFAULT_DAG_SIGNING_CADENCE)
    parser.add_argument("--agent-key-rotation-cadence", type=int, default=None)
    parser.add_argument("--key-rotation-cadence", type=int, default=None)
    parser.add_argument("--dag-key-rotation-cadence", type=int, default=0)
    parser.add_argument("--key-mode", choices=KEY_MODES, default="separate")
    parser.add_argument("--max-dag-nodes", type=int, default=DEFAULT_MAX_DAG_NODES)
    parser.add_argument("--seal-final-partial", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--include-events", action="store_true", help="reserved for future raw event output")
    args = parser.parse_args(argv)

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")
    counts = [args.messages] if args.messages is not None else args.counts
    if any(count <= 0 for count in counts):
        raise SystemExit("counts/messages must be > 0")
    transports = list(TRANSPORTS if args.transport == "both" else (args.transport,))
    try:
        payload = comparison_payload(transports, args.mode, counts, args.runs, args)
    except ValueError as exc:
        parser.error(str(exc))
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print_text(payload)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
