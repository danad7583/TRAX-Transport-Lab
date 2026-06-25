from __future__ import annotations

from dataclasses import dataclass

from .dag_model import DemoDagNode
from .metrics import RunMetrics


@dataclass
class TransportDemoResult:
    ok: bool
    transport: str
    dag_nodes: list[DemoDagNode]
    final_tip: bytes | None
    log_lines: list[str]
    metrics: RunMetrics
    error: str | None = None
