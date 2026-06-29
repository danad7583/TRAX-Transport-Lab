from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
import platform
import secrets
import socket
import struct
import sys
import time
import uuid
from typing import Any


PROTOCOL = "trax-peer-v0"
TRAX_VERSION = "0.1.0"
DAG_CONFIG_VERSION = "0"
MAX_HEADER_LEN = 65_536
DEFAULT_MAX_PAYLOAD_SIZE = 134_217_728
DEFAULT_LISTEN_BACKLOG = 1
DEFAULT_SAFETY_TIMEOUT_SECONDS = 30.0

FRAME_GENESIS_START = "GENESIS_START_V0"
FRAME_GENESIS_ACCEPT = "GENESIS_ACCEPT_V0"
FRAME_GENESIS_READY = "GENESIS_READY_V0"
FRAME_REQUEST = "REQUEST_V0"
FRAME_RESPONSE = "RESPONSE_V0"
FRAME_TRAFFIC_STOP = "TRAFFIC_STOP_V0"
FRAME_FINAL_RESPONSE = "FINAL_RESPONSE_V0"


class PeerProtocolError(ValueError):
    """Raised when a TRAX peer frame violates the lab protocol."""


@dataclass(frozen=True)
class PeerConfig:
    node_id: str
    listen_host: str
    listen_port: int
    peer_host: str
    peer_port: int
    mode: str = "dag-genesis"
    duration_seconds: float = 60.0
    payload_size: int = 1_048_576
    chunk_size: int = 1_400
    dag_signing_cadence: int = 8
    agent_key_rotation_cadence: int = 1_000
    dag_key_rotation_cadence: int = 10_000
    key_mode: str = "shared"
    max_dag_nodes: int = 100_000
    seal_final_partial: bool = False
    initiator: bool = False
    json_output: bool = False
    max_header_len: int = MAX_HEADER_LEN
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE
    safety_timeout_seconds: float = DEFAULT_SAFETY_TIMEOUT_SECONDS


@dataclass(frozen=True)
class Frame:
    header: dict[str, Any]
    payload: bytes
    payload_len: int
    message_hash: str


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def hash_bytes(data: bytes) -> str:
    return hashlib.blake2s(data, digest_size=32).hexdigest()


def hash_obj(obj: Any) -> str:
    return hash_bytes(canonical_json(obj))


def generate_keypair() -> tuple[str, str]:
    private_key = secrets.token_hex(32)
    public_key = hash_bytes(("trax-peer-public:" + private_key).encode("utf-8"))
    return private_key, public_key


def public_key_hash(public_key: str) -> str:
    return hash_bytes(public_key.encode("ascii"))


def simulated_sign(public_key: str, material: dict[str, Any]) -> str:
    return hash_bytes(public_key.encode("ascii") + canonical_json(material))


def verify_simulated_signature(public_key: str, material: dict[str, Any], signature: str) -> bool:
    return secrets.compare_digest(simulated_sign(public_key, material), signature)


def signature_material(header: dict[str, Any], payload_hash: str) -> dict[str, Any]:
    material = {key: value for key, value in header.items() if key != "signature"}
    material["payload_hash"] = payload_hash
    return material


def sent_message_hash(header: dict[str, Any], payload_hash: str) -> str:
    return hash_obj(
        {
            "header": {key: header[key] for key in sorted(header) if key != "signature"},
            "payload_hash": payload_hash,
        }
    )


def deterministic_payload_chunks(
    *,
    session_id: str,
    run_id: str,
    request_id: int,
    direction: str,
    seed: str,
    payload_size: int,
    chunk_size: int,
):
    remaining = payload_size
    counter = 0
    while remaining > 0:
        block = hashlib.blake2b(
            f"{session_id}:{run_id}:{request_id}:{direction}:{seed}:{counter}".encode("utf-8"),
            digest_size=64,
        ).digest()
        wanted = min(remaining, chunk_size)
        while wanted > len(block):
            yield block
            remaining -= len(block)
            wanted = min(remaining, chunk_size)
            counter += 1
            block = hashlib.blake2b(
                f"{session_id}:{run_id}:{request_id}:{direction}:{seed}:{counter}".encode("utf-8"),
                digest_size=64,
            ).digest()
        yield block[:wanted]
        remaining -= wanted
        counter += 1


def deterministic_payload_hash(
    *,
    session_id: str,
    run_id: str,
    request_id: int,
    direction: str,
    seed: str,
    payload_size: int,
    chunk_size: int,
) -> str:
    digest = hashlib.blake2s(digest_size=32)
    for chunk in deterministic_payload_chunks(
        session_id=session_id,
        run_id=run_id,
        request_id=request_id,
        direction=direction,
        seed=seed,
        payload_size=payload_size,
        chunk_size=chunk_size,
    ):
        digest.update(chunk)
    return digest.hexdigest()


def make_event_hash(frame_type: str, header: dict[str, Any]) -> str:
    return hash_obj(
        {
            "type": frame_type,
            "session_id": header.get("session_id"),
            "run_id": header.get("run_id"),
            "request_id": header.get("request_id"),
            "payload_hash": header.get("payload_hash"),
            "previous_tip": header.get("previous_tip"),
        }
    )


def next_tip(previous_tip: str, event_hash: str) -> str:
    return hash_obj({"previous_tip": previous_tip, "event_hash": event_hash})


def read_exact(sock: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
    remaining = n
    while remaining:
        try:
            chunk = sock.recv(remaining)
        except socket.timeout as exc:
            raise PeerProtocolError(f"timed out while reading {n} bytes") from exc
        if not chunk:
            raise PeerProtocolError(f"truncated frame: {remaining} bytes remaining")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_peer_frame(
    sock: socket.socket,
    *,
    max_header_len: int = MAX_HEADER_LEN,
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE,
    collect_payload: bool = False,
) -> Frame:
    raw_header_len = read_exact(sock, 4)
    (header_len,) = struct.unpack(">I", raw_header_len)
    if header_len == 0:
        raise PeerProtocolError("zero-length header")
    if header_len > max_header_len:
        raise PeerProtocolError(f"oversized header: {header_len}")

    header_bytes = read_exact(sock, header_len)
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PeerProtocolError("malformed JSON header") from exc
    if not isinstance(header, dict):
        raise PeerProtocolError("header JSON must be an object")
    if header.get("protocol") != PROTOCOL:
        raise PeerProtocolError("unsupported protocol")

    raw_payload_len = read_exact(sock, 8)
    (payload_len,) = struct.unpack(">Q", raw_payload_len)
    if payload_len > max_payload_size:
        raise PeerProtocolError(f"payload larger than max_payload_size: {payload_len}")
    if "payload_size" in header and int(header["payload_size"]) != payload_len:
        raise PeerProtocolError("payload size mismatch")

    digest = hashlib.blake2s(digest_size=32)
    payload_parts: list[bytes] = []
    remaining = payload_len
    while remaining:
        chunk = read_exact(sock, min(remaining, 65_536))
        digest.update(chunk)
        if collect_payload:
            payload_parts.append(chunk)
        remaining -= len(chunk)

    actual_payload_hash = digest.hexdigest()
    expected_payload_hash = header.get("payload_hash")
    if expected_payload_hash is not None and expected_payload_hash != actual_payload_hash:
        raise PeerProtocolError("payload hash mismatch")

    message_hash = hash_obj(
        {
            "header": {key: header[key] for key in sorted(header) if key != "signature"},
            "payload_hash": actual_payload_hash,
        }
    )
    return Frame(
        header=header,
        payload=b"".join(payload_parts),
        payload_len=payload_len,
        message_hash=message_hash,
    )


def send_peer_frame(
    sock: socket.socket,
    header: dict[str, Any],
    payload: bytes = b"",
    *,
    payload_chunks=None,
    payload_size: int | None = None,
) -> int:
    if payload_chunks is None:
        payload_size = len(payload)
        payload_chunks = [payload]
    elif payload_size is None:
        raise ValueError("payload_size is required when payload_chunks is provided")

    header_bytes = canonical_json(header)
    if len(header_bytes) == 0 or len(header_bytes) > MAX_HEADER_LEN:
        raise PeerProtocolError("invalid header length")

    sent = 4 + len(header_bytes) + 8 + int(payload_size)
    sock.sendall(struct.pack(">I", len(header_bytes)))
    sock.sendall(header_bytes)
    sock.sendall(struct.pack(">Q", int(payload_size)))
    for chunk in payload_chunks:
        if chunk:
            sock.sendall(chunk)
    return sent


def create_metrics(config: PeerConfig, role: str) -> dict[str, Any]:
    return {
        "role": role,
        "node_id": config.node_id,
        "peer_host": config.peer_host,
        "peer_port": config.peer_port,
        "listen_host": config.listen_host,
        "listen_port": config.listen_port,
        "mode": config.mode,
        "protocol": PROTOCOL,
        "session_id": None,
        "run_id": None,
        "initiator": config.initiator,
        "ok": False,
        "stop_reason": None,
        "duration_seconds_configured": config.duration_seconds,
        "duration_seconds_actual": 0.0,
        "key_mode": config.key_mode,
        "shared_key_session": config.key_mode == "shared",
        "key_mode_simulated": True,
        "dag_config_simulated": True,
        "dag_signing_cadence": config.dag_signing_cadence,
        "agent_key_rotation_cadence": config.agent_key_rotation_cadence,
        "dag_key_rotation_cadence": config.dag_key_rotation_cadence,
        "max_dag_nodes": config.max_dag_nodes,
        "seal_final_partial": config.seal_final_partial,
        "payload_size": config.payload_size,
        "chunk_size": config.chunk_size,
        "session_start_sent": False,
        "session_start_received": False,
        "genesis_start_sent": False,
        "genesis_start_received": False,
        "genesis_accept_sent": False,
        "genesis_accept_received": False,
        "genesis_ready_sent": False,
        "genesis_ready_received": False,
        "requests_sent": 0,
        "requests_received": 0,
        "responses_sent": 0,
        "responses_received": 0,
        "traffic_stop_sent": False,
        "traffic_stop_received": False,
        "final_response_sent": False,
        "final_response_received": False,
        "total_bytes_sent": 0,
        "total_bytes_received": 0,
        "payload_hash_match_count": 0,
        "payload_hash_mismatch_count": 0,
        "rejected_frame_count": 0,
        "malformed_frame_count": 0,
        "truncated_frame_count": 0,
        "duplicate_request_reject_count": 0,
        "total_wall_ms": 0.0,
        "bytes_per_second": 0.0,
        "mb_per_second": 0.0,
        "signed_genesis_create_count": 0,
        "signed_genesis_verify_count": 0,
        "hot_path_signed_packet_count": 0,
        "hash_bound_message_count": 0,
        "dag_segment_count": 0,
        "agent_key_rotation_event_count": 0,
        "agent_key_rotation_signed_packet_count": 0,
        "dag_key_rotation_event_count": 0,
        "dag_nodes_retained": 0,
        "dag_nodes_pruned": 0,
        "genesis_hash": None,
        "dag_config_hash": None,
        "accepted_genesis_hash": None,
        "accepted_dag_config_hash": None,
        "genesis_start_message_hash": None,
        "genesis_accept_previous_message_hash": None,
        "genesis_accept_message_hash": None,
        "initiator_public_key_hash": None,
        "receiver_public_key_hash": None,
        "final_tip": None,
        "dag_finalized": False,
        "dag_closed": False,
        "config_conflicts": [],
        "error": None,
        "hostname": socket.gethostname(),
        "private_ip": local_private_ip(),
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
    }


def local_private_ip() -> str | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return None


def validate_config(config: PeerConfig) -> None:
    if config.mode != "dag-genesis":
        raise PeerProtocolError("tcp_peer only supports --mode dag-genesis")
    if config.key_mode != "shared":
        raise PeerProtocolError("tcp_peer requires --key-mode shared")
    if config.dag_signing_cadence <= 0:
        raise PeerProtocolError("dag_signing_cadence must be > 0")
    if config.agent_key_rotation_cadence < 0:
        raise PeerProtocolError("agent_key_rotation_cadence must be >= 0")
    if config.dag_key_rotation_cadence < 0:
        raise PeerProtocolError("dag_key_rotation_cadence must be >= 0")
    if config.max_dag_nodes <= 0:
        raise PeerProtocolError("max_dag_nodes must be > 0")
    if config.payload_size < 0:
        raise PeerProtocolError("payload_size must be >= 0")
    if config.payload_size > config.max_payload_size:
        raise PeerProtocolError("payload_size exceeds max_payload_size")
    if config.chunk_size <= 0:
        raise PeerProtocolError("chunk_size must be > 0")


def make_genesis_payload(
    config: PeerConfig,
    *,
    session_id: str,
    run_id: str,
    initiator_public_key: str,
    receiver_node_id: str,
) -> dict[str, Any]:
    return {
        "genesis": {
            "type": "GENESIS_V0",
            "session_id": session_id,
            "run_id": run_id,
            "initiator_node_id": config.node_id,
            "receiver_node_id": receiver_node_id,
            "initiator_public_key": initiator_public_key,
            "key_mode": "shared",
            "genesis_signed_once": True,
        },
        "dag_config": {
            "dag_signing_cadence": config.dag_signing_cadence,
            "agent_key_rotation_cadence": config.agent_key_rotation_cadence,
            "dag_key_rotation_cadence": config.dag_key_rotation_cadence,
            "max_dag_nodes": config.max_dag_nodes,
            "seal_final_partial": config.seal_final_partial,
        },
        "traffic": {
            "duration_seconds": config.duration_seconds,
            "payload_size": config.payload_size,
            "chunk_size": config.chunk_size,
            "message_type": "garbage.payload.v0",
        },
        "limits": {
            "max_header_len": config.max_header_len,
            "max_payload_size": config.max_payload_size,
        },
    }


def update_tip(metrics: dict[str, Any], frame_type: str, header: dict[str, Any]) -> str:
    event_hash = header.get("event_hash") or make_event_hash(frame_type, header)
    previous_tip = metrics.get("final_tip") or ""
    final_tip = next_tip(previous_tip, event_hash)
    metrics["final_tip"] = final_tip
    retained = int(metrics["dag_nodes_retained"]) + 1
    metrics["dag_nodes_retained"] = min(retained, int(metrics["max_dag_nodes"]))
    if retained > int(metrics["max_dag_nodes"]):
        metrics["dag_nodes_pruned"] = retained - int(metrics["max_dag_nodes"])
    return final_tip


def accept_request_id(seen_request_ids: set[int], request_id: int) -> None:
    if request_id in seen_request_ids:
        raise PeerProtocolError("duplicate request_id")
    seen_request_ids.add(request_id)


def validate_continuity(
    metrics: dict[str, Any],
    frame_type: str,
    header: dict[str, Any],
) -> None:
    if header.get("previous_tip") != (metrics.get("final_tip") or ""):
        raise PeerProtocolError("invalid previous_tip")
    expected_event_hash = make_event_hash(frame_type, header)
    if header.get("event_hash") != expected_event_hash:
        raise PeerProtocolError("invalid event_hash")


def recv_counted(sock: socket.socket, metrics: dict[str, Any], **kwargs) -> Frame:
    before = int(metrics["total_bytes_received"])
    frame = read_peer_frame(sock, **kwargs)
    metrics["total_bytes_received"] = before + 4 + len(canonical_json(frame.header)) + 8 + frame.payload_len
    metrics["payload_hash_match_count"] += 1 if frame.header.get("payload_hash") else 0
    return frame


def send_counted(
    sock: socket.socket,
    metrics: dict[str, Any],
    header: dict[str, Any],
    payload: bytes = b"",
    *,
    payload_chunks=None,
    payload_size: int | None = None,
) -> None:
    metrics["total_bytes_sent"] += send_peer_frame(
        sock,
        header,
        payload,
        payload_chunks=payload_chunks,
        payload_size=payload_size,
    )


def run_initiator(config: PeerConfig) -> dict[str, Any]:
    validate_config(config)
    metrics = create_metrics(config, "initiator")
    started = time.perf_counter()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((config.listen_host, config.listen_port))
    listener.listen(DEFAULT_LISTEN_BACKLOG)
    listener.settimeout(0.2)

    private_key, public_key = generate_keypair()
    del private_key
    receiver_node_id = "peer"
    session_id = hash_bytes(os.urandom(32))
    run_id = str(uuid.uuid4())
    metrics["session_id"] = session_id
    metrics["run_id"] = run_id
    metrics["initiator_public_key_hash"] = public_key_hash(public_key)
    metrics["session_start_sent"] = True

    sock = socket.create_connection((config.peer_host, config.peer_port), timeout=config.safety_timeout_seconds)
    sock.settimeout(config.safety_timeout_seconds)
    try:
        payload_obj = make_genesis_payload(
            config,
            session_id=session_id,
            run_id=run_id,
            initiator_public_key=public_key,
            receiver_node_id=receiver_node_id,
        )
        payload = canonical_json(payload_obj)
        genesis_hash = hash_obj(payload_obj["genesis"])
        dag_config_hash = hash_obj(payload_obj["dag_config"])
        payload_hash = hash_bytes(payload)
        header = {
            "type": FRAME_GENESIS_START,
            "protocol": PROTOCOL,
            "trax_version": TRAX_VERSION,
            "dag_config_version": DAG_CONFIG_VERSION,
            "session_id": session_id,
            "run_id": run_id,
            "initiator_node_id": config.node_id,
            "receiver_node_id": receiver_node_id,
            "key_mode": "shared",
            "initiator_public_key": public_key,
            "genesis_hash": genesis_hash,
            "dag_config_hash": dag_config_hash,
            "payload_hash": payload_hash,
            "initiator_nonce": secrets.token_hex(16),
        }
        header["signature"] = simulated_sign(public_key, signature_material(header, payload_hash))
        send_counted(sock, metrics, header, payload)
        metrics["genesis_start_sent"] = True
        metrics["signed_genesis_create_count"] = 1
        metrics["genesis_hash"] = genesis_hash
        metrics["dag_config_hash"] = dag_config_hash
        genesis_start_message_hash = sent_message_hash(header, payload_hash)
        metrics["genesis_start_message_hash"] = genesis_start_message_hash
        update_tip(metrics, FRAME_GENESIS_START, {"event_hash": genesis_hash})

        accept = recv_counted(sock, metrics, collect_payload=True)
        accept_header = accept.header
        if accept_header.get("type") != FRAME_GENESIS_ACCEPT:
            raise PeerProtocolError("expected GENESIS_ACCEPT_V0")
        receiver_public_key = str(accept_header.get("receiver_public_key", ""))
        receiver_public_key_hash = public_key_hash(receiver_public_key)
        metrics["receiver_public_key_hash"] = receiver_public_key_hash
        expected_accept_payload_hash = hash_bytes(accept.payload)
        if not verify_simulated_signature(
            receiver_public_key,
            signature_material(accept_header, expected_accept_payload_hash),
            str(accept_header.get("signature", "")),
        ):
            raise PeerProtocolError("receiver signature verifies false")
        if accept_header.get("session_id") != session_id or accept_header.get("run_id") != run_id:
            raise PeerProtocolError("GENESIS_ACCEPT session/run mismatch")
        if accept_header.get("accepted_genesis_hash") != genesis_hash:
            raise PeerProtocolError("accepted_genesis_hash mismatch")
        if accept_header.get("accepted_dag_config_hash") != dag_config_hash:
            raise PeerProtocolError("accepted_dag_config_hash mismatch")
        if accept_header.get("initiator_public_key_hash") != metrics["initiator_public_key_hash"]:
            raise PeerProtocolError("initiator_public_key_hash mismatch")
        if accept_header.get("previous_message_hash") != genesis_start_message_hash:
            raise PeerProtocolError("GENESIS_ACCEPT previous_message_hash mismatch")
        metrics["genesis_accept_received"] = True
        metrics["genesis_accept_previous_message_hash"] = accept_header.get("previous_message_hash")
        metrics["genesis_accept_message_hash"] = accept.message_hash
        metrics["accepted_genesis_hash"] = accept_header.get("accepted_genesis_hash")
        metrics["accepted_dag_config_hash"] = accept_header.get("accepted_dag_config_hash")
        metrics["signed_genesis_verify_count"] = 1
        update_tip(metrics, FRAME_GENESIS_ACCEPT, {"event_hash": accept.message_hash})

        ready_header = {
            "type": FRAME_GENESIS_READY,
            "protocol": PROTOCOL,
            "session_id": session_id,
            "run_id": run_id,
            "initiator_node_id": config.node_id,
            "receiver_node_id": accept_header.get("receiver_node_id"),
            "accepted_receiver_public_key_hash": receiver_public_key_hash,
            "genesis_hash": genesis_hash,
            "dag_config_hash": dag_config_hash,
            "previous_message_hash": accept.message_hash,
        }
        ready_header["signature"] = simulated_sign(public_key, signature_material(ready_header, ""))
        send_counted(sock, metrics, ready_header)
        metrics["genesis_ready_sent"] = True
        update_tip(metrics, FRAME_GENESIS_READY, {"event_hash": sent_message_hash(ready_header, hash_bytes(b""))})

        deadline = time.perf_counter() + config.duration_seconds
        request_id = 0
        while time.perf_counter() < deadline or request_id == 0:
            request_id += 1
            payload_hash = deterministic_payload_hash(
                session_id=session_id,
                run_id=run_id,
                request_id=request_id,
                direction="request",
                seed=config.node_id,
                payload_size=config.payload_size,
                chunk_size=config.chunk_size,
            )
            previous_tip = metrics["final_tip"] or ""
            req_header = {
                "type": FRAME_REQUEST,
                "protocol": PROTOCOL,
                "session_id": session_id,
                "run_id": run_id,
                "request_id": request_id,
                "sender_node_id": config.node_id,
                "receiver_node_id": accept_header.get("receiver_node_id"),
                "payload_size": config.payload_size,
                "chunk_size": config.chunk_size,
                "payload_hash": payload_hash,
                "message_type": "garbage.payload.v0",
                "previous_tip": previous_tip,
            }
            req_header["event_hash"] = make_event_hash(FRAME_REQUEST, req_header)
            send_counted(
                sock,
                metrics,
                req_header,
                payload_chunks=deterministic_payload_chunks(
                    session_id=session_id,
                    run_id=run_id,
                    request_id=request_id,
                    direction="request",
                    seed=config.node_id,
                    payload_size=config.payload_size,
                    chunk_size=config.chunk_size,
                ),
                payload_size=config.payload_size,
            )
            metrics["requests_sent"] += 1
            metrics["hash_bound_message_count"] += 1
            update_tip(metrics, FRAME_REQUEST, req_header)

            resp = recv_counted(sock, metrics)
            resp_header = resp.header
            if resp_header.get("type") != FRAME_RESPONSE:
                raise PeerProtocolError("expected RESPONSE_V0")
            if resp_header.get("request_id") != request_id:
                raise PeerProtocolError("response request_id mismatch")
            validate_continuity(metrics, FRAME_RESPONSE, resp_header)
            metrics["responses_received"] += 1
            metrics["hash_bound_message_count"] += 1
            update_tip(metrics, FRAME_RESPONSE, resp_header)

        stop_header = {
            "type": FRAME_TRAFFIC_STOP,
            "protocol": PROTOCOL,
            "session_id": session_id,
            "run_id": run_id,
            "sender_node_id": config.node_id,
            "receiver_node_id": accept_header.get("receiver_node_id"),
            "reason": "duration_elapsed",
            "request_count": metrics["requests_sent"],
            "previous_tip": metrics["final_tip"] or "",
        }
        stop_header["event_hash"] = make_event_hash(FRAME_TRAFFIC_STOP, stop_header)
        send_counted(sock, metrics, stop_header)
        metrics["traffic_stop_sent"] = True
        update_tip(metrics, FRAME_TRAFFIC_STOP, stop_header)

        final = recv_counted(sock, metrics)
        if final.header.get("type") != FRAME_FINAL_RESPONSE:
            raise PeerProtocolError("expected FINAL_RESPONSE_V0")
        metrics["final_response_received"] = True
        metrics["stop_reason"] = "duration_elapsed"
        update_tip(metrics, FRAME_FINAL_RESPONSE, final.header)
        metrics["ok"] = True
        return finish_metrics(metrics, started)
    finally:
        sock.close()
        listener.close()


def validate_genesis_start(frame: Frame, config: PeerConfig, metrics: dict[str, Any]) -> dict[str, Any]:
    header = frame.header
    if header.get("type") != FRAME_GENESIS_START:
        raise PeerProtocolError("first frame must be GENESIS_START_V0")
    payload_obj = json.loads(frame.payload.decode("utf-8"))
    genesis = payload_obj.get("genesis", {})
    dag_config = payload_obj.get("dag_config", {})
    traffic = payload_obj.get("traffic", {})
    limits = payload_obj.get("limits", {})
    if header.get("key_mode") != "shared" or genesis.get("key_mode") != "shared":
        raise PeerProtocolError("key_mode shared required")
    if not header.get("session_id") or not header.get("initiator_nonce"):
        raise PeerProtocolError("session_id and nonce are required")
    if header.get("genesis_hash") != hash_obj(genesis):
        raise PeerProtocolError("genesis hash mismatch")
    if header.get("dag_config_hash") != hash_obj(dag_config):
        raise PeerProtocolError("dag config hash mismatch")
    public_key = str(header.get("initiator_public_key", ""))
    if not public_key:
        raise PeerProtocolError("initiator public key is required")
    if not verify_simulated_signature(
        public_key,
        signature_material(header, header["payload_hash"]),
        str(header.get("signature", "")),
    ):
        raise PeerProtocolError("initiator signature verifies false")
    if int(dag_config.get("dag_signing_cadence", 0)) <= 0:
        raise PeerProtocolError("dag_signing_cadence must be > 0")
    if int(dag_config.get("max_dag_nodes", 0)) <= 0:
        raise PeerProtocolError("max_dag_nodes must be > 0")
    if int(traffic.get("payload_size", 0)) > config.max_payload_size:
        raise PeerProtocolError("payload over receiver max payload size")
    if int(limits.get("max_header_len", MAX_HEADER_LEN)) > config.max_header_len:
        raise PeerProtocolError("initiator max_header_len exceeds receiver safety limit")

    conflicts = []
    for key in ("dag_signing_cadence", "agent_key_rotation_cadence", "dag_key_rotation_cadence", "max_dag_nodes", "seal_final_partial"):
        local_value = getattr(config, key)
        remote_value = dag_config.get(key)
        if remote_value is not None and local_value != remote_value:
            conflicts.append({"field": key, "local": local_value, "accepted": remote_value})
    metrics["config_conflicts"] = conflicts
    metrics["dag_signing_cadence"] = dag_config["dag_signing_cadence"]
    metrics["agent_key_rotation_cadence"] = dag_config["agent_key_rotation_cadence"]
    metrics["dag_key_rotation_cadence"] = dag_config["dag_key_rotation_cadence"]
    metrics["max_dag_nodes"] = dag_config["max_dag_nodes"]
    metrics["seal_final_partial"] = dag_config["seal_final_partial"]
    metrics["payload_size"] = traffic["payload_size"]
    metrics["chunk_size"] = traffic["chunk_size"]
    metrics["duration_seconds_configured"] = traffic["duration_seconds"]
    return payload_obj


def run_receiver(config: PeerConfig) -> dict[str, Any]:
    validate_config(config)
    metrics = create_metrics(config, "receiver")
    started = time.perf_counter()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((config.listen_host, config.listen_port))
    listener.listen(DEFAULT_LISTEN_BACKLOG)
    listener.settimeout(config.safety_timeout_seconds)
    try:
        conn, _addr = listener.accept()
        conn.settimeout(config.safety_timeout_seconds)
        with conn:
            metrics["session_start_received"] = True
            start_frame = recv_counted(conn, metrics, collect_payload=True)
            payload_obj = validate_genesis_start(start_frame, config, metrics)
            start_header = start_frame.header
            genesis = payload_obj["genesis"]
            dag_config = payload_obj["dag_config"]
            metrics["genesis_start_received"] = True
            metrics["session_id"] = start_header["session_id"]
            metrics["run_id"] = start_header["run_id"]
            metrics["genesis_hash"] = start_header["genesis_hash"]
            metrics["dag_config_hash"] = start_header["dag_config_hash"]
            metrics["accepted_genesis_hash"] = start_header["genesis_hash"]
            metrics["accepted_dag_config_hash"] = start_header["dag_config_hash"]
            metrics["initiator_public_key_hash"] = public_key_hash(start_header["initiator_public_key"])
            metrics["genesis_start_message_hash"] = start_frame.message_hash
            metrics["signed_genesis_verify_count"] = 1
            update_tip(metrics, FRAME_GENESIS_START, {"event_hash": start_header["genesis_hash"]})

            _receiver_private, receiver_public = generate_keypair()
            del _receiver_private
            metrics["receiver_public_key_hash"] = public_key_hash(receiver_public)
            accept_payload_obj = {
                "accepted": True,
                "session_id": start_header["session_id"],
                "run_id": start_header["run_id"],
                "receiver_node_id": config.node_id,
                "dag_config": dag_config,
            }
            accept_payload = canonical_json(accept_payload_obj)
            accept_payload_hash = hash_bytes(accept_payload)
            accept_header = {
                "type": FRAME_GENESIS_ACCEPT,
                "protocol": PROTOCOL,
                "session_id": start_header["session_id"],
                "run_id": start_header["run_id"],
                "receiver_node_id": config.node_id,
                "initiator_node_id": genesis["initiator_node_id"],
                "receiver_public_key": receiver_public,
                "initiator_public_key_hash": metrics["initiator_public_key_hash"],
                "accepted_genesis_hash": start_header["genesis_hash"],
                "accepted_dag_config_hash": start_header["dag_config_hash"],
                "receiver_nonce": secrets.token_hex(16),
                "previous_message_hash": start_frame.message_hash,
                "payload_hash": accept_payload_hash,
            }
            accept_header["signature"] = simulated_sign(receiver_public, signature_material(accept_header, accept_payload_hash))
            send_counted(conn, metrics, accept_header, accept_payload)
            metrics["genesis_accept_sent"] = True
            metrics["genesis_accept_previous_message_hash"] = start_frame.message_hash
            metrics["genesis_accept_message_hash"] = sent_message_hash(accept_header, accept_payload_hash)
            metrics["signed_genesis_create_count"] = 1
            update_tip(metrics, FRAME_GENESIS_ACCEPT, {"event_hash": metrics["genesis_accept_message_hash"]})

            ready_frame = recv_counted(conn, metrics)
            ready = ready_frame.header
            if ready.get("type") != FRAME_GENESIS_READY:
                raise PeerProtocolError("expected GENESIS_READY_V0")
            if ready.get("previous_message_hash") != ready.get("previous_message_hash") or ready.get("previous_message_hash") != ready_frame.header.get("previous_message_hash"):
                pass
            if ready.get("genesis_hash") != metrics["genesis_hash"] or ready.get("dag_config_hash") != metrics["dag_config_hash"]:
                raise PeerProtocolError("GENESIS_READY config mismatch")
            if ready.get("accepted_receiver_public_key_hash") != metrics["receiver_public_key_hash"]:
                raise PeerProtocolError("GENESIS_READY receiver key hash mismatch")
            if not verify_simulated_signature(start_header["initiator_public_key"], signature_material(ready, ""), str(ready.get("signature", ""))):
                raise PeerProtocolError("GENESIS_READY signature verifies false")
            metrics["genesis_ready_received"] = True
            update_tip(metrics, FRAME_GENESIS_READY, {"event_hash": ready_frame.message_hash})

            seen_request_ids: set[int] = set()
            while True:
                frame = recv_counted(conn, metrics)
                header = frame.header
                frame_type = header.get("type")
                if header.get("session_id") != metrics["session_id"] or header.get("run_id") != metrics["run_id"]:
                    raise PeerProtocolError("wrong session_id")
                if frame_type == FRAME_REQUEST:
                    request_id = int(header.get("request_id", 0))
                    try:
                        accept_request_id(seen_request_ids, request_id)
                    except PeerProtocolError:
                        metrics["duplicate_request_reject_count"] += 1
                        raise
                    validate_continuity(metrics, FRAME_REQUEST, header)
                    metrics["requests_received"] += 1
                    metrics["hash_bound_message_count"] += 1
                    update_tip(metrics, FRAME_REQUEST, header)
                    response_payload_hash = deterministic_payload_hash(
                        session_id=metrics["session_id"],
                        run_id=metrics["run_id"],
                        request_id=request_id,
                        direction="response",
                        seed=config.node_id,
                        payload_size=int(header["payload_size"]),
                        chunk_size=int(header["chunk_size"]),
                    )
                    resp_header = {
                        "type": FRAME_RESPONSE,
                        "protocol": PROTOCOL,
                        "session_id": metrics["session_id"],
                        "run_id": metrics["run_id"],
                        "request_id": request_id,
                        "sender_node_id": config.node_id,
                        "receiver_node_id": header["sender_node_id"],
                        "payload_size": int(header["payload_size"]),
                        "chunk_size": int(header["chunk_size"]),
                        "payload_hash": response_payload_hash,
                        "message_type": "garbage.response.v0",
                        "previous_tip": metrics["final_tip"] or "",
                    }
                    resp_header["event_hash"] = make_event_hash(FRAME_RESPONSE, resp_header)
                    send_counted(
                        conn,
                        metrics,
                        resp_header,
                        payload_chunks=deterministic_payload_chunks(
                            session_id=metrics["session_id"],
                            run_id=metrics["run_id"],
                            request_id=request_id,
                            direction="response",
                            seed=config.node_id,
                            payload_size=int(header["payload_size"]),
                            chunk_size=int(header["chunk_size"]),
                        ),
                        payload_size=int(header["payload_size"]),
                    )
                    metrics["responses_sent"] += 1
                    metrics["hash_bound_message_count"] += 1
                    update_tip(metrics, FRAME_RESPONSE, resp_header)
                elif frame_type == FRAME_TRAFFIC_STOP:
                    validate_continuity(metrics, FRAME_TRAFFIC_STOP, header)
                    metrics["traffic_stop_received"] = True
                    update_tip(metrics, FRAME_TRAFFIC_STOP, header)
                    final_header = {
                        "type": FRAME_FINAL_RESPONSE,
                        "protocol": PROTOCOL,
                        "session_id": metrics["session_id"],
                        "run_id": metrics["run_id"],
                        "sender_node_id": config.node_id,
                        "receiver_node_id": header["sender_node_id"],
                        "accepted_request_count": metrics["requests_received"],
                        "accepted_response_count": metrics["responses_sent"],
                        "rejected_frame_count": metrics["rejected_frame_count"],
                        "final_tip": metrics["final_tip"] or "",
                        "previous_tip": metrics["final_tip"] or "",
                    }
                    final_header["event_hash"] = make_event_hash(FRAME_FINAL_RESPONSE, final_header)
                    send_counted(conn, metrics, final_header)
                    metrics["final_response_sent"] = True
                    update_tip(metrics, FRAME_FINAL_RESPONSE, final_header)
                    metrics["stop_reason"] = "traffic_stop_received"
                    metrics["ok"] = True
                    return finish_metrics(metrics, started)
                else:
                    raise PeerProtocolError(f"unsupported frame type: {frame_type}")
    except PeerProtocolError as exc:
        metrics["rejected_frame_count"] += 1
        if "malformed" in str(exc):
            metrics["malformed_frame_count"] += 1
        if "truncated" in str(exc):
            metrics["truncated_frame_count"] += 1
        metrics["error"] = str(exc)
        metrics["stop_reason"] = "protocol_error"
        return finish_metrics(metrics, started)
    finally:
        listener.close()


def finish_metrics(metrics: dict[str, Any], started: float) -> dict[str, Any]:
    elapsed = max(0.0, time.perf_counter() - started)
    metrics["duration_seconds_actual"] = elapsed
    metrics["total_wall_ms"] = elapsed * 1000.0
    total_bytes = int(metrics["total_bytes_sent"]) + int(metrics["total_bytes_received"])
    metrics["bytes_per_second"] = total_bytes / elapsed if elapsed else 0.0
    metrics["mb_per_second"] = metrics["bytes_per_second"] / (1024 * 1024)
    if metrics["agent_key_rotation_cadence"]:
        metrics["agent_key_rotation_event_count"] = metrics["requests_received"] // metrics["agent_key_rotation_cadence"]
    if metrics["dag_key_rotation_cadence"]:
        metrics["dag_key_rotation_event_count"] = metrics["dag_nodes_retained"] // metrics["dag_key_rotation_cadence"]
    if metrics["dag_signing_cadence"]:
        messages = metrics["requests_sent"] or metrics["requests_received"]
        metrics["dag_segment_count"] = messages // metrics["dag_signing_cadence"]
    return metrics


def run_peer(config: PeerConfig) -> dict[str, Any]:
    try:
        return run_initiator(config) if config.initiator else run_receiver(config)
    except Exception as exc:
        metrics = create_metrics(config, "initiator" if config.initiator else "receiver")
        metrics["error"] = str(exc)
        metrics["stop_reason"] = "error"
        return finish_metrics(metrics, time.perf_counter())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRAX bidirectional TCP peer lab")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=39100)
    parser.add_argument("--peer-host", required=True)
    parser.add_argument("--peer-port", type=int, default=39100)
    parser.add_argument("--mode", default="dag-genesis")
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--payload-size", type=int, default=1_048_576)
    parser.add_argument("--chunk-size", type=int, default=1_400)
    parser.add_argument("--dag-signing-cadence", type=int, default=8)
    parser.add_argument("--agent-key-rotation-cadence", type=int, default=1_000)
    parser.add_argument("--dag-key-rotation-cadence", type=int, default=10_000)
    parser.add_argument("--key-mode", default="shared")
    parser.add_argument("--max-dag-nodes", type=int, default=100_000)
    parser.add_argument("--seal-final-partial", action="store_true")
    parser.add_argument("--initiator", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    parser.add_argument("--max-header-len", type=int, default=MAX_HEADER_LEN)
    parser.add_argument("--max-payload-size", type=int, default=DEFAULT_MAX_PAYLOAD_SIZE)
    parser.add_argument("--safety-timeout-seconds", type=float, default=DEFAULT_SAFETY_TIMEOUT_SECONDS)
    return parser


def config_from_args(args: argparse.Namespace) -> PeerConfig:
    return PeerConfig(
        node_id=args.node_id,
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        peer_host=args.peer_host,
        peer_port=args.peer_port,
        mode=args.mode,
        duration_seconds=args.duration_seconds,
        payload_size=args.payload_size,
        chunk_size=args.chunk_size,
        dag_signing_cadence=args.dag_signing_cadence,
        agent_key_rotation_cadence=args.agent_key_rotation_cadence,
        dag_key_rotation_cadence=args.dag_key_rotation_cadence,
        key_mode=args.key_mode,
        max_dag_nodes=args.max_dag_nodes,
        seal_final_partial=args.seal_final_partial,
        initiator=args.initiator,
        json_output=args.json_output,
        max_header_len=args.max_header_len,
        max_payload_size=args.max_payload_size,
        safety_timeout_seconds=args.safety_timeout_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = config_from_args(args)
    metrics = run_peer(config)
    if config.json_output:
        print(json.dumps(metrics, sort_keys=True, separators=(",", ":")))
    else:
        print(f"TRAX TCP PEER {'OK' if metrics['ok'] else 'FAILED'}")
        for key in ("role", "node_id", "session_id", "run_id", "requests_sent", "requests_received", "responses_sent", "responses_received", "final_tip", "error"):
            print(f"{key}: {metrics.get(key)}")
    return 0 if metrics["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
