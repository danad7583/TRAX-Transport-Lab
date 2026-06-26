from __future__ import annotations

import argparse
import json
import struct
import socket
import threading
from time import perf_counter_ns
from typing import Any

from .dag_model import DemoDag, DemoDagNode
from .framing import (
    MAX_PACKET_LEN,
    FramingError,
    recv_frame,
    send_frame,
)
from .logging_utils import DemoLog
from .metrics import CATEGORY_ORCHESTRATION, CATEGORY_TRAX, RunMetrics
from .messages import (
    MessageError,
    canonical_json,
    decode_message,
    encode_message,
    hex_to_bytes,
)
from .trax_adapter import TraxAdapter
from .transport_common import (
    TransportDemoResult,
    print_repeated_json,
    print_repeated_text,
    run_repeated,
)


INIT_SESSION_ID_SEED = b"TRAX_TRANSPORT_LAB_INIT_SESSION_V0"
JUNK_PAYLOAD = b"TRAX_TEST_STREAM_BLOCK_0001" * 128
SOCKET_TIMEOUT_SECONDS = 3.0
SIGNED_ENVELOPE_MODE = "signed-envelope"
CHECKPOINT_MODE = "checkpoint"
DAG_GENESIS_MODE = "dag-genesis"
MODE_CHOICES = (SIGNED_ENVELOPE_MODE, CHECKPOINT_MODE, DAG_GENESIS_MODE)


TcpDemoResult = TransportDemoResult


class ProtocolError(RuntimeError):
    def __init__(self, message_type: str, reason: str):
        super().__init__(reason)
        self.message_type = message_type
        self.reason = reason


def security_payload(message_type: str, fields: dict[str, Any]) -> bytes:
    return canonical_json({"message_type": message_type, **fields})


def _packet_hash(adapter: TraxAdapter, message: dict[str, Any]) -> bytes:
    return adapter.hash32(encode_message(message, metrics=adapter.metrics))


def _send_tcp(sock: socket.socket, message: dict[str, Any], metrics: RunMetrics) -> None:
    data = encode_message(message, metrics=metrics)
    send_frame(sock, data, metrics=metrics)


def _recv_tcp(sock: socket.socket, metrics: RunMetrics) -> dict[str, Any]:
    data = recv_frame(sock, MAX_PACKET_LEN, metrics=metrics)
    return decode_message(data, metrics=metrics)


def _validate_security_message(
    adapter: TraxAdapter,
    message: dict[str, Any],
    expected_type: str,
    receiver_public_key: bytes,
    payload: bytes,
    expected_session_id: bytes | None = None,
    purpose: str = "signed_envelope",
) -> None:
    message_type = message.get("message_type", "<missing>")
    if message_type != expected_type:
        raise ProtocolError(str(message_type), f"expected {expected_type}")
    if expected_session_id is not None:
        session_id = hex_to_bytes(message["session_id"], "session_id")
        if session_id != expected_session_id:
            raise ProtocolError(expected_type, "wrong session_id")
    if hex_to_bytes(message["receiver_public_key"], "receiver_public_key") != receiver_public_key:
        raise ProtocolError(expected_type, "wrong receiver_public_key")
    envelope = hex_to_bytes(message["admission_envelope"], "admission_envelope")
    if purpose == "checkpoint":
        event_name = "checkpoint.verify_signed_checkpoint"
    elif purpose == "genesis":
        event_name = "dag_genesis.verify_signed_genesis"
    else:
        event_name = "signed_envelope.verify"
    with adapter.metrics.measure(event_name, CATEGORY_TRAX):
        verified = adapter.verify_for_receiver(envelope, payload, receiver_public_key)
    if purpose == "checkpoint":
        adapter.metrics.add_signed_checkpoint_verify()
    elif purpose == "genesis":
        adapter.metrics.add_signed_genesis_verify()
    else:
        adapter.metrics.add_signed_envelope_verify()
        adapter.metrics.add_hot_path_signed_packet()
    if not verified:
        raise ProtocolError(expected_type, "admission envelope verification failed")
    decoded = adapter.decode_envelope(envelope)
    if decoded.get("message_type") not in (None, expected_type):
        raise ProtocolError(expected_type, "envelope message_type mismatch")


def _make_security_message(
    adapter: TraxAdapter,
    message_type: str,
    sender_private_key,
    sender_public_key: bytes,
    receiver_public_key: bytes,
    session_id: bytes,
    payload: bytes,
    dag_parent_refs: list[bytes] | None = None,
    purpose: str = "signed_envelope",
    **extra: Any,
) -> dict[str, Any]:
    if purpose == "checkpoint":
        event_name = "checkpoint.create_signed_checkpoint"
    elif purpose == "genesis":
        event_name = "dag_genesis.create_signed_genesis"
    else:
        event_name = "signed_envelope.create"
    with adapter.metrics.measure(event_name, CATEGORY_TRAX):
        envelope = adapter.create_envelope(
            sender_private_key,
            receiver_public_key,
            session_id,
            payload,
            message_type,
            dag_parent_refs=dag_parent_refs,
        )
    if purpose == "checkpoint":
        adapter.metrics.add_signed_checkpoint_create()
    elif purpose == "genesis":
        adapter.metrics.add_signed_genesis_create()
    else:
        adapter.metrics.add_signed_envelope_create()
        adapter.metrics.add_hot_path_signed_packet()
    return {
        "message_type": message_type,
        "session_id": session_id.hex(),
        "sender_public_key": sender_public_key.hex(),
        "receiver_public_key": receiver_public_key.hex(),
        "payload_hash": adapter.hash32(payload).hex(),
        "dag_parent_refs": [ref.hex() for ref in dag_parent_refs or []],
        "admission_envelope": envelope.hex(),
        **extra,
    }


def _hash_bound_material(message: dict[str, Any]) -> bytes:
    return canonical_json(
        {
            key: value
            for key, value in message.items()
            if key not in {"event_hash", "demo_protocol", "demo_version"}
        }
    )


def _make_hash_bound_message(
    adapter: TraxAdapter,
    message_type: str,
    sender_public_key: bytes,
    receiver_public_key: bytes,
    session_id: bytes,
    payload_hash: bytes,
    counter: int,
    previous_tip: bytes,
    mode: str = CHECKPOINT_MODE,
    genesis_tip: bytes | None = None,
    **extra: Any,
) -> dict[str, Any]:
    with adapter.metrics.measure("hash_bound.message", CATEGORY_TRAX):
        message = {
            "message_type": message_type,
            "mode": mode,
            "session_id": session_id.hex(),
            "sender_public_key": sender_public_key.hex(),
            "receiver_public_key": receiver_public_key.hex(),
            "payload_hash": payload_hash.hex(),
            "admission_envelope": "",
            "cycle_index": extra.pop("cycle_index", 0),
            "counter": counter,
            "previous_tip": previous_tip.hex(),
            "dag_parent_refs": [previous_tip.hex()],
            **extra,
        }
        if genesis_tip is not None:
            message["genesis_tip"] = genesis_tip.hex()
        message["event_hash"] = adapter.hash32(_hash_bound_material(message)).hex()
        adapter.metrics.add_hash_bound_message()
        return message


def _validate_hash_bound_message(
    adapter: TraxAdapter,
    message: dict[str, Any],
    expected_type: str,
    session_id: bytes,
    payload_hash: bytes,
    counter: int,
    previous_tip: bytes,
    receiver_public_key: bytes | None = None,
    mode: str = CHECKPOINT_MODE,
    genesis_tip: bytes | None = None,
) -> None:
    with adapter.metrics.measure("hash_bound.message", CATEGORY_TRAX):
        message_type = message.get("message_type", "<missing>")
        if message_type != expected_type:
            raise ProtocolError(str(message_type), f"expected {expected_type}")
        if message.get("mode", CHECKPOINT_MODE) != mode:
            raise ProtocolError(expected_type, "wrong mode")
        if hex_to_bytes(message["session_id"], "session_id") != session_id:
            raise ProtocolError(expected_type, "wrong session_id")
        if message.get("counter") != counter:
            raise ProtocolError(expected_type, "wrong counter")
        if hex_to_bytes(message["previous_tip"], "previous_tip") != previous_tip:
            raise ProtocolError(expected_type, "wrong previous_tip")
        if hex_to_bytes(message["payload_hash"], "payload_hash") != payload_hash:
            raise ProtocolError(expected_type, "payload_hash mismatch")
        if receiver_public_key is not None:
            actual_receiver = hex_to_bytes(message["receiver_public_key"], "receiver_public_key")
            if actual_receiver != receiver_public_key:
                raise ProtocolError(expected_type, "wrong receiver_public_key")
        if genesis_tip is not None:
            actual_genesis_tip = hex_to_bytes(message["genesis_tip"], "genesis_tip")
            if actual_genesis_tip != genesis_tip:
                raise ProtocolError(expected_type, "wrong genesis_tip")
        expected_hash = adapter.hash32(_hash_bound_material(message)).hex()
        if message.get("event_hash") != expected_hash:
            raise ProtocolError(expected_type, "event_hash mismatch")


def _make_signed_genesis(
    adapter: TraxAdapter,
    sender_private_key,
    sender_public_key: bytes,
    receiver_public_key: bytes,
    session_id: bytes,
    client_nonce: bytes,
    server_nonce: bytes,
) -> dict[str, Any]:
    content = {
        "protocol": "TRAX",
        "mode": DAG_GENESIS_MODE,
        "session_id": session_id.hex(),
        "client_identity_or_key_ref": sender_public_key.hex(),
        "server_identity_or_key_ref": receiver_public_key.hex(),
        "client_nonce": client_nonce.hex(),
        "server_nonce": server_nonce.hex(),
        "initial_counter": 1,
        "policy": "hash-bound-dag-continuity",
    }
    genesis_payload = security_payload("GENESIS_V0", content)
    genesis_hash = adapter.hash32(genesis_payload)
    return _make_security_message(
        adapter,
        "TRAX_GENESIS",
        sender_private_key,
        sender_public_key,
        receiver_public_key,
        session_id,
        genesis_payload,
        purpose="genesis",
        mode=DAG_GENESIS_MODE,
        genesis_hash=genesis_hash.hex(),
        client_nonce=client_nonce.hex(),
        server_nonce=server_nonce.hex(),
    )


def _genesis_payload_from_message(message: dict[str, Any]) -> bytes:
    return security_payload(
        "GENESIS_V0",
        {
            "protocol": "TRAX",
            "mode": DAG_GENESIS_MODE,
            "session_id": message["session_id"],
            "client_identity_or_key_ref": message["sender_public_key"],
            "server_identity_or_key_ref": message["receiver_public_key"],
            "client_nonce": message["client_nonce"],
            "server_nonce": message["server_nonce"],
            "initial_counter": 1,
            "policy": "hash-bound-dag-continuity",
        },
    )


def _validate_signed_genesis(
    adapter: TraxAdapter,
    message: dict[str, Any],
    receiver_public_key: bytes,
    expected_session_id: bytes,
) -> bytes:
    payload = _genesis_payload_from_message(message)
    if adapter.hash32(payload).hex() != message["genesis_hash"]:
        raise ProtocolError("TRAX_GENESIS", "genesis hash mismatch")
    _validate_security_message(
        adapter,
        message,
        "TRAX_GENESIS",
        receiver_public_key,
        payload,
        expected_session_id,
        purpose="genesis",
    )
    return adapter.hash32(encode_message(message, metrics=adapter.metrics))


def _make_checkpoint(
    adapter: TraxAdapter,
    transport: str,
    sender_private_key,
    sender_public_key: bytes,
    receiver_public_key: bytes,
    session_id: bytes,
    start_tip: bytes,
    final_tip_before_checkpoint: bytes,
    message_hashes: list[bytes],
    first_counter: int,
    last_counter: int,
) -> dict[str, Any]:
    aggregate_hash = adapter.hash32(b"".join(message_hashes))
    content = {
        "mode": CHECKPOINT_MODE,
        "transport": transport,
        "session_id": session_id.hex(),
        "start_tip": start_tip.hex(),
        "final_tip_before_checkpoint": final_tip_before_checkpoint.hex(),
        "event_count": 3,
        "message_count": len(message_hashes),
        "first_counter": first_counter,
        "last_counter": last_counter,
        "aggregate_hash": aggregate_hash.hex(),
    }
    checkpoint_payload = security_payload("TRAX_CHECKPOINT", content)
    checkpoint_content_hash = adapter.hash32(checkpoint_payload)
    return _make_security_message(
        adapter,
        "TRAX_CHECKPOINT",
        sender_private_key,
        sender_public_key,
        receiver_public_key,
        session_id,
        checkpoint_payload,
        dag_parent_refs=[final_tip_before_checkpoint],
        purpose="checkpoint",
        transport=transport,
        checkpoint_content_hash=checkpoint_content_hash.hex(),
        aggregate_hash=aggregate_hash.hex(),
        start_tip=start_tip.hex(),
        final_tip_before_checkpoint=final_tip_before_checkpoint.hex(),
        first_counter=first_counter,
        last_counter=last_counter,
        message_count=len(message_hashes),
    )


def _checkpoint_payload_from_message(message: dict[str, Any]) -> bytes:
    return security_payload(
        "TRAX_CHECKPOINT",
        {
            "mode": CHECKPOINT_MODE,
            "transport": message["transport"] if "transport" in message else "tcp",
            "session_id": message["session_id"],
            "start_tip": message["start_tip"],
            "final_tip_before_checkpoint": message["final_tip_before_checkpoint"],
            "event_count": 3,
            "message_count": message["message_count"],
            "first_counter": message["first_counter"],
            "last_counter": message["last_counter"],
            "aggregate_hash": message["aggregate_hash"],
        },
    )


def _server(
    listener: socket.socket,
    adapter: TraxAdapter,
    dag: DemoDag,
    log: DemoLog,
    metrics: RunMetrics,
    server_keys,
    mode: str,
) -> None:
    conn: socket.socket | None = None
    server_started_ns = perf_counter_ns()
    try:
        with metrics.measure("server.accept_init", CATEGORY_ORCHESTRATION):
            conn, _ = listener.accept()
            conn.settimeout(SOCKET_TIMEOUT_SECONDS)
            init_session_id = adapter.hash32(INIT_SESSION_ID_SEED)

            genesis = None
            genesis_tip = None
            if mode == DAG_GENESIS_MODE:
                with metrics.measure("TRAX_GENESIS", CATEGORY_ORCHESTRATION):
                    genesis = _recv_tcp(conn, metrics)
                client_public_key = hex_to_bytes(
                    genesis["sender_public_key"], "sender_public_key", metrics=metrics
                )
                client_nonce = hex_to_bytes(genesis["client_nonce"], "client_nonce", metrics=metrics)
                server_nonce = hex_to_bytes(genesis["server_nonce"], "server_nonce", metrics=metrics)
                session_id = init_session_id
                genesis_tip = _validate_signed_genesis(
                    adapter,
                    genesis,
                    server_keys.public_key,
                    session_id,
                )
                log.add("TRAX_GENESIS accepted")

            with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                init = _recv_tcp(conn, metrics)
            if mode == DAG_GENESIS_MODE:
                init_payload = security_payload(
                    "TRAX_INIT",
                    {
                        "client_nonce": client_nonce.hex(),
                        "client_public_key": client_public_key.hex(),
                        "server_public_key": server_keys.public_key.hex(),
                    },
                )
                _validate_hash_bound_message(
                    adapter,
                    init,
                    "TRAX_INIT",
                    session_id,
                    adapter.hash32(init_payload),
                    1,
                    genesis_tip,
                    server_keys.public_key,
                    mode=DAG_GENESIS_MODE,
                    genesis_tip=genesis_tip,
                )
            else:
                client_public_key = hex_to_bytes(init["sender_public_key"], "sender_public_key", metrics=metrics)
                client_nonce = hex_to_bytes(init["client_nonce"], "client_nonce", metrics=metrics)
                init_payload = security_payload(
                    "TRAX_INIT",
                    {
                        "client_nonce": client_nonce.hex(),
                        "client_public_key": client_public_key.hex(),
                        "server_public_key": server_keys.public_key.hex(),
                    },
                )
                _validate_security_message(
                    adapter,
                    init,
                    "TRAX_INIT",
                    server_keys.public_key,
                    init_payload,
                    init_session_id,
                )
            log.add("TRAX_INIT accepted")

        if mode == DAG_GENESIS_MODE:
            ack_payload = security_payload(
                "TRAX_INIT_ACK",
                {
                    "client_nonce": client_nonce.hex(),
                    "server_nonce": server_nonce.hex(),
                    "client_public_key": client_public_key.hex(),
                    "server_public_key": server_keys.public_key.hex(),
                    "session_id": session_id.hex(),
                },
            )
            init_ack = _make_hash_bound_message(
                adapter,
                "TRAX_INIT_ACK",
                server_keys.public_key,
                client_public_key,
                session_id,
                adapter.hash32(ack_payload),
                2,
                genesis_tip,
                mode=DAG_GENESIS_MODE,
                genesis_tip=genesis_tip,
                client_nonce=client_nonce.hex(),
                server_nonce=server_nonce.hex(),
            )
        else:
            server_nonce = adapter.generate_nonce()
            transcript_hash = adapter.hash32(client_public_key + server_keys.public_key)
            session_id = adapter.derive_session_id(transcript_hash, client_nonce, server_nonce)
            ack_payload = security_payload(
                "TRAX_INIT_ACK",
                {
                    "client_nonce": client_nonce.hex(),
                    "server_nonce": server_nonce.hex(),
                    "client_public_key": client_public_key.hex(),
                    "server_public_key": server_keys.public_key.hex(),
                    "session_id": session_id.hex(),
                },
            )
            init_ack = _make_security_message(
                adapter,
                "TRAX_INIT_ACK",
                server_keys.private_key,
                server_keys.public_key,
                client_public_key,
                session_id,
                ack_payload,
                client_nonce=client_nonce.hex(),
                server_nonce=server_nonce.hex(),
            )
        with metrics.measure("TRAX_INIT_ACK", CATEGORY_ORCHESTRATION):
            _send_tcp(conn, init_ack, metrics)
        log.add("TRAX_INIT_ACK sent")

        with metrics.measure("TRAX_COMMIT", CATEGORY_ORCHESTRATION):
            commit = _recv_tcp(conn, metrics)
        commit_payload = security_payload(
            "TRAX_COMMIT",
            {
                "client_public_key": client_public_key.hex(),
                "server_public_key": server_keys.public_key.hex(),
                "session_id": session_id.hex(),
            },
        )
        if mode == DAG_GENESIS_MODE:
            _validate_hash_bound_message(
                adapter,
                commit,
                "TRAX_COMMIT",
                session_id,
                adapter.hash32(commit_payload),
                3,
                genesis_tip,
                server_keys.public_key,
                mode=DAG_GENESIS_MODE,
                genesis_tip=genesis_tip,
            )
        else:
            _validate_security_message(
                adapter,
                commit,
                "TRAX_COMMIT",
                server_keys.public_key,
                commit_payload,
                session_id,
            )
        log.add("TRAX_COMMIT accepted")
        with metrics.measure("dag_append_SESSION_START_V0", CATEGORY_ORCHESTRATION):
            session_packets = {
                "TRAX_INIT": _packet_hash(adapter, init),
                "TRAX_INIT_ACK": _packet_hash(adapter, init_ack),
                "TRAX_COMMIT": _packet_hash(adapter, commit),
            }
            if genesis is not None:
                session_packets["TRAX_GENESIS"] = _packet_hash(adapter, genesis)
            session_node = dag.append_node(
                "SESSION_START_V0",
                session_id,
                [],
                session_packets,
            )
            metrics.add_dag_node(session_node.node_hash)
        log.add("SESSION_START_V0 appended")

        with metrics.measure("TRAX_REQ", CATEGORY_ORCHESTRATION):
            req = _recv_tcp(conn, metrics)
        committed_payload_hash = hex_to_bytes(req["payload_hash"], "payload_hash")
        if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
            _validate_hash_bound_message(
                adapter,
                req,
                "TRAX_REQ",
                session_id,
                committed_payload_hash,
                1,
                session_node.node_hash,
                server_keys.public_key,
                mode=mode,
                genesis_tip=genesis_tip,
            )
        else:
            req_payload = security_payload(
                "TRAX_REQ",
                {
                    "cycle_index": req["cycle_index"],
                    "payload_hash": committed_payload_hash.hex(),
                    "session_id": session_id.hex(),
                },
            )
            _validate_security_message(
                adapter,
                req,
                "TRAX_REQ",
                server_keys.public_key,
                req_payload,
                session_id,
            )
        log.add("TRAX_REQ accepted")

        if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
            req_ack = _make_hash_bound_message(
                adapter,
                "TRAX_REQ_ACK",
                server_keys.public_key,
                client_public_key,
                session_id,
                committed_payload_hash,
                2,
                session_node.node_hash,
                mode=mode,
                genesis_tip=genesis_tip,
                cycle_index=req["cycle_index"],
            )
        else:
            req_ack_payload = security_payload(
                "TRAX_REQ_ACK",
                {
                    "cycle_index": req["cycle_index"],
                    "payload_hash": committed_payload_hash.hex(),
                    "session_id": session_id.hex(),
                },
            )
            req_ack = _make_security_message(
                adapter,
                "TRAX_REQ_ACK",
                server_keys.private_key,
                server_keys.public_key,
                client_public_key,
                session_id,
                req_ack_payload,
                dag_parent_refs=[session_node.node_hash],
                cycle_index=req["cycle_index"],
                payload_hash=committed_payload_hash.hex(),
            )
        with metrics.measure("TRAX_REQ_ACK", CATEGORY_ORCHESTRATION):
            _send_tcp(conn, req_ack, metrics)
        log.add("TRAX_REQ_ACK sent")

        with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
            junk = _recv_tcp(conn, metrics)
        if junk["message_type"] != "JUNK_STREAM_PAYLOAD":
            raise ProtocolError(junk["message_type"], "expected JUNK_STREAM_PAYLOAD")
        if hex_to_bytes(junk["session_id"], "session_id", metrics=metrics) != session_id:
            raise ProtocolError("JUNK_STREAM_PAYLOAD", "wrong session_id")
        if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
            _validate_hash_bound_message(
                adapter,
                junk,
                "JUNK_STREAM_PAYLOAD",
                session_id,
                committed_payload_hash,
                3,
                session_node.node_hash,
                mode=mode,
                genesis_tip=genesis_tip,
            )
        payload = hex_to_bytes(junk["payload"], "payload", metrics=metrics)
        metrics.set_payload_bytes(len(payload))
        with metrics.measure("payload_hash_verify", CATEGORY_TRAX):
            if adapter.hash32(payload) != committed_payload_hash:
                raise ProtocolError("JUNK_STREAM_PAYLOAD", "payload hash mismatch")
        log.add("JUNK_STREAM_PAYLOAD hash verified")

        if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
            res_ack = _make_hash_bound_message(
                adapter,
                "TRAX_RES_ACK",
                server_keys.public_key,
                client_public_key,
                session_id,
                committed_payload_hash,
                4,
                session_node.node_hash,
                mode=mode,
                genesis_tip=genesis_tip,
                cycle_index=req["cycle_index"],
            )
        else:
            res_payload = security_payload(
                "TRAX_RES_ACK",
                {
                    "cycle_index": req["cycle_index"],
                    "payload_hash": committed_payload_hash.hex(),
                    "session_id": session_id.hex(),
                },
            )
            res_ack = _make_security_message(
                adapter,
                "TRAX_RES_ACK",
                server_keys.private_key,
                server_keys.public_key,
                client_public_key,
                session_id,
                res_payload,
                dag_parent_refs=[session_node.node_hash],
                cycle_index=req["cycle_index"],
                payload_hash=committed_payload_hash.hex(),
            )
        with metrics.measure("TRAX_RES_ACK", CATEGORY_ORCHESTRATION):
            _send_tcp(conn, res_ack, metrics)
        log.add("TRAX_RES_ACK sent")
        with metrics.measure("dag_append_STREAM_EXCHANGE_V0", CATEGORY_ORCHESTRATION):
            stream_node = dag.append_node(
                "STREAM_EXCHANGE_V0",
                session_id,
                [session_node.node_hash],
                {
                    "TRAX_REQ": _packet_hash(adapter, req),
                    "TRAX_REQ_ACK": _packet_hash(adapter, req_ack),
                    "JUNK_STREAM_PAYLOAD": _packet_hash(adapter, junk),
                    "TRAX_RES_ACK": _packet_hash(adapter, res_ack),
                },
            )
            metrics.add_dag_node(stream_node.node_hash)
        log.add("STREAM_EXCHANGE_V0 appended")
        if mode == CHECKPOINT_MODE:
            checkpoint = _make_checkpoint(
                adapter,
                "tcp",
                server_keys.private_key,
                server_keys.public_key,
                client_public_key,
                session_id,
                session_node.node_hash,
                stream_node.node_hash,
                [
                    _packet_hash(adapter, req),
                    _packet_hash(adapter, req_ack),
                    _packet_hash(adapter, junk),
                    _packet_hash(adapter, res_ack),
                ],
                1,
                4,
            )
            with metrics.measure("TRAX_CHECKPOINT", CATEGORY_ORCHESTRATION):
                _send_tcp(conn, checkpoint, metrics)
            log.add("TRAX_CHECKPOINT sent")
            with metrics.measure("dag_append_CHECKPOINT_V0", CATEGORY_ORCHESTRATION):
                checkpoint_node = dag.append_node(
                    "CHECKPOINT_V0",
                    session_id,
                    [stream_node.node_hash],
                    {
                        "TRAX_CHECKPOINT": _packet_hash(adapter, checkpoint),
                    },
                )
                metrics.add_dag_node(checkpoint_node.node_hash)
            log.add("CHECKPOINT_V0 appended")
    except ProtocolError as exc:
        log.reject(exc.message_type, exc.reason)
    except (MessageError, FramingError, OSError, KeyError, ValueError) as exc:
        log.reject("<unknown>", str(exc))
    finally:
        server_ended_ns = perf_counter_ns()
        metrics.record_event(
            "server.total",
            CATEGORY_ORCHESTRATION,
            server_started_ns,
            server_ended_ns,
        )
        if conn is not None:
            conn.close()
        listener.close()


def run_tcp_demo(
    adverse_case: str | None = None,
    adapter: TraxAdapter | None = None,
    mode: str = SIGNED_ENVELOPE_MODE,
) -> TcpDemoResult:
    if mode not in MODE_CHOICES:
        raise ValueError(f"unsupported mode={mode!r}")
    metrics = RunMetrics("tcp", mode=mode)
    demo_started_ns = perf_counter_ns()
    adapter = adapter or TraxAdapter(metrics=metrics)
    adapter.metrics = metrics
    dag = DemoDag(metrics=metrics)
    log = DemoLog()
    with metrics.measure("demo.thread_startup", CATEGORY_ORCHESTRATION):
        server_keys = adapter.generate_keypair()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(SOCKET_TIMEOUT_SECONDS)
    host, port = listener.getsockname()

    server_thread = threading.Thread(
        target=_server,
        args=(listener, adapter, dag, log, metrics, server_keys, mode),
        daemon=True,
    )
    with metrics.measure("server.thread_start", CATEGORY_ORCHESTRATION):
        server_thread.start()

    error: str | None = None
    try:
        with metrics.measure("client.total", CATEGORY_ORCHESTRATION):
            _client(host, port, adapter, log, metrics, adverse_case, server_keys.public_key, mode)
    except ProtocolError as exc:
        log.reject(exc.message_type, exc.reason)
        error = exc.reason
    except (MessageError, FramingError, OSError, KeyError, ValueError) as exc:
        log.reject("<client>", str(exc))
        error = str(exc)

    with metrics.measure("server.thread_join", CATEGORY_ORCHESTRATION):
        server_thread.join(SOCKET_TIMEOUT_SECONDS + 1.0)
    if server_thread.is_alive():
        error = error or "server thread did not finish"
        log.reject("<server>", error)

    expected_nodes = 3 if mode == CHECKPOINT_MODE else 2
    ok = error is None and len(dag) == expected_nodes and not any(
        line.startswith("rejected ") for line in log.lines
    )
    demo_ended_ns = perf_counter_ns()
    metrics.record_event(
        "demo.total",
        CATEGORY_ORCHESTRATION,
        demo_started_ns,
        demo_ended_ns,
        ok=ok,
    )
    metrics.finish(dag.final_tip())
    if ok:
        log.add("")
        log.add("Final tip:")
        log.add(dag.final_tip().hex() if dag.final_tip() else "<none>")
        log.add("")
        log.lines.extend(metrics.summary_lines())
    return TcpDemoResult(
        ok=ok,
        transport="tcp",
        dag_nodes=dag.enumerate(),
        final_tip=dag.final_tip(),
        log_lines=list(log.lines),
        metrics=metrics,
        error=error,
    )


def _client(
    host: str,
    port: int,
    adapter: TraxAdapter,
    log: DemoLog,
    metrics: RunMetrics,
    adverse_case: str | None,
    observed_server_public_key: bytes,
    mode: str,
) -> None:
    with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT_SECONDS) as sock:
        sock.settimeout(SOCKET_TIMEOUT_SECONDS)
        client_keys = adapter.generate_keypair()
        init_session_id = adapter.hash32(INIT_SESSION_ID_SEED)
        client_nonce = adapter.generate_nonce()

        if adverse_case == "oversized_frame":
            sock.sendall(struct.pack(">I", MAX_PACKET_LEN + 1))
            metrics.add_frame_sent()
            metrics.add_bytes_sent(4)
            log.add("TRAX_INIT sent")
            return
        if adverse_case == "truncated_frame":
            sock.sendall(struct.pack(">I", 32) + b"short")
            metrics.add_frame_sent()
            metrics.add_bytes_sent(9)
            log.add("TRAX_INIT sent")
            return
        init_payload = security_payload(
            "TRAX_INIT",
            {
                "client_nonce": client_nonce.hex(),
                "client_public_key": client_keys.public_key.hex(),
                "server_public_key": observed_server_public_key.hex(),
            },
        )
        init_receiver = observed_server_public_key
        if adverse_case == "wrong_receiver_init":
            init_receiver = adapter.generate_keypair().public_key
        genesis = None
        genesis_tip = None
        if mode == DAG_GENESIS_MODE:
            server_nonce = adapter.generate_nonce()
            session_id = init_session_id
            genesis = _make_signed_genesis(
                adapter,
                client_keys.private_key,
                client_keys.public_key,
                init_receiver,
                session_id,
                client_nonce,
                server_nonce,
            )
            genesis_tip = _packet_hash(adapter, genesis)
            init = _make_hash_bound_message(
                adapter,
                "TRAX_INIT",
                client_keys.public_key,
                init_receiver,
                session_id,
                adapter.hash32(init_payload),
                1,
                genesis_tip,
                mode=DAG_GENESIS_MODE,
                genesis_tip=genesis_tip,
                client_nonce=client_nonce.hex(),
            )
        else:
            init = _make_security_message(
                adapter,
                "TRAX_INIT",
                client_keys.private_key,
                client_keys.public_key,
                init_receiver,
                init_session_id,
                init_payload,
                client_nonce=client_nonce.hex(),
            )
        if adverse_case == "malformed_init":
            sock.sendall(b"\x00\x00\x00\x09not-json!")
            metrics.add_frame_sent()
            metrics.add_bytes_sent(13)
            log.add("TRAX_INIT sent")
            return

        with metrics.measure("session_handshake_total", CATEGORY_ORCHESTRATION):
            if mode == DAG_GENESIS_MODE:
                with metrics.measure("TRAX_GENESIS", CATEGORY_ORCHESTRATION):
                    _send_tcp(sock, genesis, metrics)
                log.add("TRAX_GENESIS sent")
            with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                _send_tcp(sock, init, metrics)
            log.add("TRAX_INIT sent")

            if adverse_case == "req_before_commit":
                bad_payload_hash = adapter.hash32(JUNK_PAYLOAD)
                bad_req_payload = security_payload(
                    "TRAX_REQ",
                    {
                        "cycle_index": 0,
                        "payload_hash": bad_payload_hash.hex(),
                        "session_id": init_session_id.hex(),
                    },
                )
                bad_req = _make_security_message(
                    adapter,
                    "TRAX_REQ",
                    client_keys.private_key,
                    client_keys.public_key,
                    observed_server_public_key,
                    init_session_id,
                    bad_req_payload,
                    cycle_index=0,
                )
                with metrics.measure("TRAX_REQ", CATEGORY_ORCHESTRATION):
                    _send_tcp(sock, bad_req, metrics)
                log.add("TRAX_REQ sent")
                return

            with metrics.measure("TRAX_INIT_ACK", CATEGORY_ORCHESTRATION):
                init_ack = _recv_tcp(sock, metrics)
            server_public_key = hex_to_bytes(init_ack["sender_public_key"], "sender_public_key")
            server_nonce = hex_to_bytes(init_ack["server_nonce"], "server_nonce")
            if mode != DAG_GENESIS_MODE:
                transcript_hash = adapter.hash32(client_keys.public_key + server_public_key)
                session_id = adapter.derive_session_id(transcript_hash, client_nonce, server_nonce)
            ack_payload = security_payload(
                "TRAX_INIT_ACK",
                {
                    "client_nonce": client_nonce.hex(),
                    "server_nonce": server_nonce.hex(),
                    "client_public_key": client_keys.public_key.hex(),
                    "server_public_key": server_public_key.hex(),
                    "session_id": session_id.hex(),
                },
            )
            if adverse_case == "bad_init_ack":
                init_ack["admission_envelope"] = "00"
            if mode == DAG_GENESIS_MODE:
                _validate_hash_bound_message(
                    adapter,
                    init_ack,
                    "TRAX_INIT_ACK",
                    session_id,
                    adapter.hash32(ack_payload),
                    2,
                    genesis_tip,
                    client_keys.public_key,
                    mode=DAG_GENESIS_MODE,
                    genesis_tip=genesis_tip,
                )
            else:
                _validate_security_message(
                    adapter,
                    init_ack,
                    "TRAX_INIT_ACK",
                    client_keys.public_key,
                    ack_payload,
                    session_id,
                )
            log.add("TRAX_INIT_ACK accepted")

            commit_payload = security_payload(
                "TRAX_COMMIT",
                {
                    "client_public_key": client_keys.public_key.hex(),
                    "server_public_key": server_public_key.hex(),
                    "session_id": session_id.hex(),
                },
            )
            commit_session_id = session_id
            if adverse_case == "wrong_session":
                commit_session_id = adapter.hash32(b"wrong-session")
            if mode == DAG_GENESIS_MODE:
                commit = _make_hash_bound_message(
                    adapter,
                    "TRAX_COMMIT",
                    client_keys.public_key,
                    server_public_key,
                    commit_session_id,
                    adapter.hash32(commit_payload),
                    3,
                    genesis_tip,
                    mode=DAG_GENESIS_MODE,
                    genesis_tip=genesis_tip,
                )
            else:
                commit = _make_security_message(
                    adapter,
                    "TRAX_COMMIT",
                    client_keys.private_key,
                    client_keys.public_key,
                    server_public_key,
                    commit_session_id,
                    commit_payload,
                )
            if adverse_case == "bad_commit":
                commit["admission_envelope"] = "00"
            with metrics.measure("TRAX_COMMIT", CATEGORY_ORCHESTRATION):
                _send_tcp(sock, commit, metrics)
            log.add("TRAX_COMMIT sent")
            if adverse_case in {"bad_commit", "wrong_session"}:
                return

        client_session_packets = {
            "TRAX_INIT": _packet_hash(adapter, init),
            "TRAX_INIT_ACK": _packet_hash(adapter, init_ack),
            "TRAX_COMMIT": _packet_hash(adapter, commit),
        }
        if genesis is not None:
            client_session_packets["TRAX_GENESIS"] = _packet_hash(adapter, genesis)
        client_session_node = DemoDag().append_node(
            "SESSION_START_V0",
            session_id,
            [],
            client_session_packets,
        )
        payload_hash = adapter.hash32(JUNK_PAYLOAD)
        if adverse_case == "payload_before_ack":
            junk = {
                "message_type": "JUNK_STREAM_PAYLOAD",
                "session_id": session_id.hex(),
                "payload": JUNK_PAYLOAD.hex(),
                "payload_hash": payload_hash.hex(),
                "cycle_index": 0,
            }
            with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
                _send_tcp(sock, junk, metrics)
            log.add("JUNK_STREAM_PAYLOAD sent")
            return

        with metrics.measure("stream_exchange_total", CATEGORY_ORCHESTRATION):
            previous_tip = client_session_node.node_hash
            if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
                # The server checks this against the actual DAG tip. The client learns
                # the authoritative tip when the checkpoint returns.
                req = _make_hash_bound_message(
                    adapter,
                    "TRAX_REQ",
                    client_keys.public_key,
                    server_public_key,
                    session_id,
                    payload_hash,
                    1,
                    previous_tip,
                    mode=mode,
                    genesis_tip=genesis_tip,
                    cycle_index=0,
                )
            else:
                req_payload = security_payload(
                    "TRAX_REQ",
                    {
                        "cycle_index": 0,
                        "payload_hash": payload_hash.hex(),
                        "session_id": session_id.hex(),
                    },
                )
                req = _make_security_message(
                    adapter,
                    "TRAX_REQ",
                    client_keys.private_key,
                    client_keys.public_key,
                    server_public_key,
                    session_id,
                    req_payload,
                    cycle_index=0,
                    payload_hash=payload_hash.hex(),
                )
            if adverse_case == "wrong_message_order":
                req["message_type"] = "TRAX_RES_ACK"
            with metrics.measure("TRAX_REQ", CATEGORY_ORCHESTRATION):
                _send_tcp(sock, req, metrics)
            log.add("TRAX_REQ sent")
            if adverse_case == "wrong_message_order":
                return

            with metrics.measure("TRAX_REQ_ACK", CATEGORY_ORCHESTRATION):
                req_ack = _recv_tcp(sock, metrics)
            if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
                _validate_hash_bound_message(
                    adapter,
                    req_ack,
                    "TRAX_REQ_ACK",
                    session_id,
                    payload_hash,
                    2,
                    previous_tip,
                    client_keys.public_key,
                    mode=mode,
                    genesis_tip=genesis_tip,
                )
                req["previous_tip"] = previous_tip.hex()
                req["dag_parent_refs"] = [previous_tip.hex()]
                req["event_hash"] = adapter.hash32(_hash_bound_material(req)).hex()
                junk = _make_hash_bound_message(
                    adapter,
                    "JUNK_STREAM_PAYLOAD",
                    client_keys.public_key,
                    server_public_key,
                    session_id,
                    payload_hash,
                    3,
                    previous_tip,
                    mode=mode,
                    genesis_tip=genesis_tip,
                    cycle_index=0,
                    payload=JUNK_PAYLOAD.hex(),
                )
            else:
                junk = {
                    "message_type": "JUNK_STREAM_PAYLOAD",
                    "session_id": session_id.hex(),
                    "payload": JUNK_PAYLOAD.hex(),
                    "payload_hash": payload_hash.hex(),
                    "cycle_index": 0,
                }
                req_ack_payload = security_payload(
                    "TRAX_REQ_ACK",
                    {
                        "cycle_index": 0,
                        "payload_hash": payload_hash.hex(),
                        "session_id": session_id.hex(),
                    },
                )
                _validate_security_message(
                    adapter,
                    req_ack,
                    "TRAX_REQ_ACK",
                    client_keys.public_key,
                    req_ack_payload,
                    session_id,
                )
            log.add("TRAX_REQ_ACK accepted")

            if adverse_case == "payload_hash_mismatch":
                junk["payload"] = (JUNK_PAYLOAD + b"tampered").hex()
            with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
                _send_tcp(sock, junk, metrics)
            log.add("JUNK_STREAM_PAYLOAD sent")
            if adverse_case == "payload_hash_mismatch":
                return

            with metrics.measure("TRAX_RES_ACK", CATEGORY_ORCHESTRATION):
                res_ack = _recv_tcp(sock, metrics)
            if mode in {CHECKPOINT_MODE, DAG_GENESIS_MODE}:
                _validate_hash_bound_message(
                    adapter,
                    res_ack,
                    "TRAX_RES_ACK",
                    session_id,
                    payload_hash,
                    4,
                    previous_tip,
                    client_keys.public_key,
                    mode=mode,
                    genesis_tip=genesis_tip,
                )
            else:
                res_ack_payload = security_payload(
                    "TRAX_RES_ACK",
                    {
                        "cycle_index": 0,
                        "payload_hash": payload_hash.hex(),
                        "session_id": session_id.hex(),
                    },
                )
                _validate_security_message(
                    adapter,
                    res_ack,
                    "TRAX_RES_ACK",
                    client_keys.public_key,
                    res_ack_payload,
                    session_id,
                )
            log.add("TRAX_RES_ACK accepted")

            if mode == CHECKPOINT_MODE:
                with metrics.measure("TRAX_CHECKPOINT", CATEGORY_ORCHESTRATION):
                    checkpoint = _recv_tcp(sock, metrics)
                checkpoint_payload = _checkpoint_payload_from_message(checkpoint)
                _validate_security_message(
                    adapter,
                    checkpoint,
                    "TRAX_CHECKPOINT",
                    client_keys.public_key,
                    checkpoint_payload,
                    session_id,
                    purpose="checkpoint",
                )
                log.add("TRAX_CHECKPOINT accepted")


def _append_dag_output(dag: DemoDag, log: DemoLog) -> None:
    log.add("")
    log.add("DAG:")
    for node in dag.enumerate():
        log.add(f"{node.index} {node.node_type:<20} {node.node_hash.hex()}")
    final_tip = dag.final_tip()
    log.add("")
    log.add(f"Final tip: {final_tip.hex() if final_tip else '<none>'}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the TRAX TCP transport demo.")
    parser.add_argument("--json", action="store_true", help="emit result metrics as JSON")
    parser.add_argument("--include-events", action="store_true", help="include raw metric events in JSON")
    parser.add_argument("--runs", type=int, default=1, help="run the demo N times")
    parser.add_argument("--mode", choices=MODE_CHOICES, default=SIGNED_ENVELOPE_MODE)
    args = parser.parse_args(argv)

    def run_selected_mode() -> TcpDemoResult:
        return run_tcp_demo(mode=args.mode)

    if args.runs != 1:
        results = run_repeated(run_selected_mode, args.runs)
        if args.json:
            print_repeated_json(results, include_events=args.include_events)
        else:
            print_repeated_text(results)
        return 0 if all(result.ok for result in results) else 1

    result = run_selected_mode()
    if args.json:
        payload = {
            "ok": result.ok,
            **result.metrics.as_dict(include_events=args.include_events),
        }
        print(json.dumps(payload, sort_keys=True))
        return 0 if result.ok else 1

    if result.ok:
        print("TCP DEMO OK")
        print()
    else:
        print("TCP DEMO FAILED")
        print()
    for line in result.log_lines:
        print(line)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
