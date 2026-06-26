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
from trax_transport_lab.tcp_demo import run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def run_many(fn, runs: int):
    return [fn() for _ in range(runs)]


def comparison_payload(tcp_results, udp_results) -> dict:
    tcp_metrics = [result.metrics for result in tcp_results]
    udp_metrics = [result.metrics for result in udp_results]
    return {
        "note": "local loopback diagnostic metrics; not benchmark-grade results",
        "runs": len(tcp_results),
        "ok": all(result.ok for result in tcp_results + udp_results),
        "tcp": summarize_metric_runs(tcp_metrics),
        "udp": summarize_metric_runs(udp_metrics),
    }


def print_text(payload: dict) -> None:
    print("TRAX Transport Lab comparison")
    print("local loopback diagnostic metrics; not benchmark-grade results")
    print("event-sum buckets may overlap across client/server threads and nested operations")
    print()
    print(f"runs: {payload['runs']}")
    print(f"ok: {payload['ok']}")
    print()
    print("Wall-clock averages:")
    for metric_name in [
        "total_wall_ms",
        "session_handshake_wall_ms",
        "stream_exchange_wall_ms",
    ]:
        tcp_avg = payload["tcp"][metric_name]["avg"]
        udp_avg = payload["udp"][metric_name]["avg"]
        print(f"TCP avg {metric_name}: {tcp_avg:.3f}")
        print(f"UDP avg {metric_name}: {udp_avg:.3f}")
    print()
    print("Event-sum averages, may overlap:")
    for metric_name in [
        "trax_primitives_event_ms",
        "python_packaging_event_ms",
        "transport_io_event_ms",
        "dag_event_ms",
    ]:
        tcp_avg = payload["tcp"][metric_name]["avg"]
        udp_avg = payload["udp"][metric_name]["avg"]
        print(f"TCP avg {metric_name}: {tcp_avg:.3f}")
        print(f"UDP avg {metric_name}: {udp_avg:.3f}")
    print()
    print("Micro highlights:")
    tcp_avg = payload["tcp"]["payload_hash_verify_us"]["avg"]
    udp_avg = payload["udp"]["payload_hash_verify_us"]["avg"]
    print(f"TCP avg payload_hash_verify_us: {tcp_avg:.3f}")
    print(f"UDP avg payload_hash_verify_us: {udp_avg:.3f}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare TCP and UDP local loopback metrics.")
    parser.add_argument("--runs", type=int, default=5, help="number of runs per transport")
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    args = parser.parse_args(argv)

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    tcp_results = run_many(run_tcp_demo, args.runs)
    udp_results = run_many(run_udp_demo, args.runs)
    payload = comparison_payload(tcp_results, udp_results)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print_text(payload)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
