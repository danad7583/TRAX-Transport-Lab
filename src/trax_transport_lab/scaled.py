from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import time
from typing import Any

from .metrics import CATEGORY_DAG, CATEGORY_ORCHESTRATION, CATEGORY_TRAX, RunMetrics
from .messages import canonical_json


KEY_MODES = ("separate", "shared", "derived")
DEFAULT_DAG_SIGNING_CADENCE = 8
DEFAULT_MAX_DAG_NODES = 100_000
MAX_MESSAGES = 10_000_000


@dataclass(frozen=True)
class ScaleConfig:
    messages: int | None = None
    dag_signing_cadence: int = DEFAULT_DAG_SIGNING_CADENCE
    agent_key_rotation_cadence: int = 0
    key_rotation_cadence_alias_value: int | None = None
    dag_key_rotation_cadence: int = 0
    key_mode: str = "separate"
    max_dag_nodes: int = DEFAULT_MAX_DAG_NODES
    seal_final_partial: bool = False
    alias_warning: str | None = None


def add_scale_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--messages", type=int, default=None)
    parser.add_argument("--dag-signing-cadence", type=int, default=DEFAULT_DAG_SIGNING_CADENCE)
    parser.add_argument("--agent-key-rotation-cadence", type=int, default=None)
    parser.add_argument("--key-rotation-cadence", type=int, default=None)
    parser.add_argument("--dag-key-rotation-cadence", type=int, default=0)
    parser.add_argument("--key-mode", choices=KEY_MODES, default="separate")
    parser.add_argument("--max-dag-nodes", type=int, default=DEFAULT_MAX_DAG_NODES)
    parser.add_argument("--seal-final-partial", action="store_true")


def scale_config_from_args(args: argparse.Namespace) -> ScaleConfig:
    return make_scale_config(
        messages=args.messages,
        dag_signing_cadence=args.dag_signing_cadence,
        agent_key_rotation_cadence=args.agent_key_rotation_cadence,
        key_rotation_cadence_alias_value=args.key_rotation_cadence,
        dag_key_rotation_cadence=args.dag_key_rotation_cadence,
        key_mode=args.key_mode,
        max_dag_nodes=args.max_dag_nodes,
        seal_final_partial=args.seal_final_partial,
    )


def make_scale_config(
    *,
    messages: int | None = None,
    dag_signing_cadence: int = DEFAULT_DAG_SIGNING_CADENCE,
    agent_key_rotation_cadence: int | None = None,
    key_rotation_cadence_alias_value: int | None = None,
    dag_key_rotation_cadence: int = 0,
    key_mode: str = "separate",
    max_dag_nodes: int = DEFAULT_MAX_DAG_NODES,
    seal_final_partial: bool = False,
) -> ScaleConfig:
    if messages is not None and messages <= 0:
        raise ValueError("messages must be > 0 when provided")
    if messages is not None and messages > MAX_MESSAGES:
        raise ValueError(f"messages must be <= {MAX_MESSAGES}")
    if dag_signing_cadence <= 0:
        raise ValueError("dag-signing-cadence must be > 0")
    if agent_key_rotation_cadence is not None and agent_key_rotation_cadence < 0:
        raise ValueError("agent-key-rotation-cadence must be >= 0")
    if key_rotation_cadence_alias_value is not None and key_rotation_cadence_alias_value < 0:
        raise ValueError("key-rotation-cadence must be >= 0")
    if dag_key_rotation_cadence < 0:
        raise ValueError("dag-key-rotation-cadence must be >= 0")
    if max_dag_nodes <= 0:
        raise ValueError("max-dag-nodes must be > 0")
    if key_mode not in KEY_MODES:
        raise ValueError("key-mode must be separate, shared, or derived")

    alias_warning = None
    if agent_key_rotation_cadence is None:
        resolved_agent_cadence = key_rotation_cadence_alias_value or 0
    else:
        resolved_agent_cadence = agent_key_rotation_cadence
        if key_rotation_cadence_alias_value is not None:
            alias_warning = (
                "key-rotation-cadence ignored because "
                "agent-key-rotation-cadence was provided"
            )

    return ScaleConfig(
        messages=messages,
        dag_signing_cadence=dag_signing_cadence,
        agent_key_rotation_cadence=resolved_agent_cadence,
        key_rotation_cadence_alias_value=key_rotation_cadence_alias_value,
        dag_key_rotation_cadence=dag_key_rotation_cadence,
        key_mode=key_mode,
        max_dag_nodes=max_dag_nodes,
        seal_final_partial=seal_final_partial,
        alias_warning=alias_warning,
    )


def _hash_obj(obj: dict[str, Any]) -> bytes:
    return hashlib.blake2s(canonical_json(obj), digest_size=32).digest()


def _complete_segment_count(messages: int, cadence: int, seal_final_partial: bool) -> int:
    count = messages // cadence
    if seal_final_partial and messages % cadence:
        count += 1
    return count


def apply_scale_metadata(metrics: RunMetrics, config: ScaleConfig) -> None:
    metrics.messages = config.messages
    metrics.dag_signing_cadence = config.dag_signing_cadence
    metrics.agent_key_rotation_cadence = config.agent_key_rotation_cadence
    metrics.key_rotation_cadence_alias_value = config.key_rotation_cadence_alias_value
    metrics.dag_key_rotation_cadence = config.dag_key_rotation_cadence
    metrics.key_mode = config.key_mode
    metrics.key_mode_simulated = True
    metrics.max_dag_nodes = config.max_dag_nodes
    metrics.seal_final_partial = config.seal_final_partial
    if config.alias_warning is not None:
        metrics.warnings.append(config.alias_warning)


def simulate_scaled_messages(
    *,
    mode: str,
    metrics: RunMetrics,
    config: ScaleConfig,
    session_id: bytes,
    initial_tip: bytes,
) -> bytes:
    apply_scale_metadata(metrics, config)
    if config.messages is None:
        metrics.update_dag_retention()
        return initial_tip

    started = time.perf_counter_ns()
    current_tip = initial_tip
    messages = config.messages
    agent_rotations = 0
    dag_key_rotations = 0
    segments = _complete_segment_count(
        messages,
        config.dag_signing_cadence,
        config.seal_final_partial,
    )

    with metrics.measure("post_genesis_total", CATEGORY_ORCHESTRATION):
        for counter in range(1, messages + 1):
            payload_hash = _hash_obj(
                {
                    "payload": "scaled-message",
                    "session_id": session_id.hex(),
                    "counter": counter,
                }
            )
            event_obj = {
                "message_type": "SCALED_MESSAGE_V0",
                "session_id": session_id.hex(),
                "counter": counter,
                "previous_tip": current_tip.hex(),
                "payload_hash": payload_hash.hex(),
                "genesis_binding": metrics.final_tip,
            }
            event_hash = _hash_obj(event_obj)
            with metrics.measure("hash_bound.message", CATEGORY_TRAX):
                metrics.add_hash_bound_message()
            with metrics.measure("dag.append_node", CATEGORY_DAG):
                current_tip = _hash_obj(
                    {
                        **event_obj,
                        "event_hash": event_hash.hex(),
                    }
                )
                metrics.add_dag_node(current_tip)
                metrics.track_dag_node_retention()

            if mode == "signed-envelope":
                with metrics.measure("signed_envelope.create", CATEGORY_TRAX):
                    pass
                with metrics.measure("signed_envelope.verify", CATEGORY_TRAX):
                    pass
                metrics.add_signed_envelope_create()
                metrics.add_signed_envelope_verify()
                metrics.add_hot_path_signed_packet()
                metrics.add_hot_path_signed_packet()

            if (
                config.agent_key_rotation_cadence > 0
                and counter % config.agent_key_rotation_cadence == 0
            ):
                agent_rotations += 1
                with metrics.measure("agent_key_rotation.event", CATEGORY_DAG):
                    current_tip = _hash_obj(
                        {
                            "message_type": "AGENT_KEY_ROTATION_V0",
                            "session_id": session_id.hex(),
                            "counter": counter,
                            "previous_tip": current_tip.hex(),
                            "old_agent_key_ref": f"agent-{agent_rotations - 1}",
                            "new_agent_public_key_hash": _hash_obj(
                                {"agent_rotation": agent_rotations}
                            ).hex(),
                            "rotation_nonce": _hash_obj(
                                {"agent_rotation_nonce": agent_rotations}
                            ).hex(),
                            "rotation_policy": "scaled-lab-simulated",
                        }
                    )
                    metrics.add_dag_node(current_tip)
                    metrics.track_dag_node_retention()
                    metrics.add_agent_key_rotation_event()
                    metrics.add_agent_key_rotation_create()
                    metrics.add_agent_key_rotation_verify()
                    metrics.add_agent_key_rotation_signed_packet()

            if (
                config.dag_key_rotation_cadence > 0
                and counter % config.dag_key_rotation_cadence == 0
            ):
                dag_key_rotations += 1
                with metrics.measure("dag_key_rotation.event", CATEGORY_DAG):
                    current_tip = _hash_obj(
                        {
                            "message_type": "DAG_KEY_ROTATION_V0",
                            "session_id": session_id.hex(),
                            "counter": counter,
                            "previous_tip": current_tip.hex(),
                            "old_dag_key_ref": f"dag-{dag_key_rotations - 1}",
                            "new_dag_public_key_ref_hash": _hash_obj(
                                {"dag_rotation": dag_key_rotations}
                            ).hex(),
                            "rotation_policy": "scaled-lab-simulated",
                        }
                    )
                    metrics.add_dag_node(current_tip)
                    metrics.track_dag_node_retention()
                    metrics.add_dag_key_rotation_event()
                    metrics.add_dag_key_rotation_create()
                    metrics.add_dag_key_rotation_verify()

        for segment in range(1, segments + 1):
            with metrics.measure("dag_segment.create", CATEGORY_DAG):
                pass
            with metrics.measure("dag_segment.verify", CATEGORY_DAG):
                pass
            metrics.add_dag_segment_create()
            metrics.add_dag_segment_verify()

    ended = time.perf_counter_ns()
    metrics.post_genesis_wall_ns += max(0, ended - started)
    metrics.dag_segment_count = segments
    metrics.update_dag_retention()
    return current_tip
