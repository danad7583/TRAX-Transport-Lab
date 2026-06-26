from __future__ import annotations

from dataclasses import dataclass
import json

from .dag_model import DemoDagNode
from .metrics import RunMetrics, aggregate_summary_lines, summarize_metric_runs


@dataclass
class TransportDemoResult:
    ok: bool
    transport: str
    dag_nodes: list[DemoDagNode]
    final_tip: bytes | None
    log_lines: list[str]
    metrics: RunMetrics
    error: str | None = None


def run_repeated(run_once, runs: int) -> list[TransportDemoResult]:
    if runs < 1:
        raise ValueError("runs must be >= 1")
    return [run_once() for _ in range(runs)]


def repeated_result_payload(
    results: list[TransportDemoResult],
    include_events: bool = False,
) -> dict:
    metrics_runs = [result.metrics for result in results]
    payload = {
        "ok": all(result.ok for result in results),
        "runs": len(results),
        "transport": results[0].transport if results else "<none>",
        "mode": results[0].metrics.mode if results else "<none>",
        "final_tips": [
            result.final_tip.hex() if result.final_tip is not None else None
            for result in results
        ],
        "aggregate": summarize_metric_runs(metrics_runs),
        "per_run": [
            {
                "ok": result.ok,
                "final_tip": result.final_tip.hex() if result.final_tip else None,
                "metrics": result.metrics.as_dict(include_events=include_events),
            }
            for result in results
        ],
    }
    return payload


def print_repeated_text(results: list[TransportDemoResult]) -> None:
    if not results:
        return
    transport = results[0].transport
    mode = results[0].metrics.mode
    print(f"{transport.upper()} repeated runs")
    print(f"mode: {mode}")
    print()
    for index, result in enumerate(results, start=1):
        tip = result.final_tip.hex() if result.final_tip else "<none>"
        print(f"run {index}: ok={result.ok} final_tip={tip}")
    print()
    for line in aggregate_summary_lines(transport, [result.metrics for result in results]):
        print(line)


def print_repeated_json(
    results: list[TransportDemoResult],
    include_events: bool = False,
) -> None:
    print(json.dumps(repeated_result_payload(results, include_events=include_events), sort_keys=True))
