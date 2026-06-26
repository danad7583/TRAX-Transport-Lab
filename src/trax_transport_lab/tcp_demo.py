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
    if not adapter.verify_for_receiver(envelope, payload, receiver_public_key):
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
    **extra: Any,
) -> dict[str, Any]:
    envelope = adapter.create_envelope(
        sender_private_key,
        receiver_public_key,
        session_id,
        payload,
        message_type,
        dag_parent_refs=dag_parent_refs,
    )
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


def _server(
    listener: socket.socket,
    adapter: TraxAdapter,
    dag: DemoDag,
    log: DemoLog,
    metrics: RunMetrics,
    server_keys,
) -> None:
    conn: socket.socket | None = None
    server_started_ns = perf_counter_ns()
    try:
        with metrics.measure("server.accept_init", CATEGORY_ORCHESTRATION):
            conn, _ = listener.accept()
            conn.settimeout(SOCKET_TIMEOUT_SECONDS)
            init_session_id = adapter.hash32(INIT_SESSION_ID_SEED)

            with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                init = _recv_tcp(conn, metrics)
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
            session_node = dag.append_node(
                "SESSION_START_V0",
                session_id,
                [],
                {
                    "TRAX_INIT": _packet_hash(adapter, init),
                    "TRAX_INIT_ACK": _packet_hash(adapter, init_ack),
                    "TRAX_COMMIT": _packet_hash(adapter, commit),
                },
            )
            metrics.add_dag_node(session_node.node_hash)
        log.add("SESSION_START_V0 appended")

        with metrics.measure("TRAX_REQ", CATEGORY_ORCHESTRATION):
            req = _recv_tcp(conn, metrics)
        committed_payload_hash = hex_to_bytes(req["payload_hash"], "payload_hash")
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
        payload = hex_to_bytes(junk["payload"], "payload", metrics=metrics)
        metrics.set_payload_bytes(len(payload))
        with metrics.measure("payload_hash_verify", CATEGORY_TRAX):
            if adapter.hash32(payload) != committed_payload_hash:
                raise ProtocolError("JUNK_STREAM_PAYLOAD", "payload hash mismatch")
        log.add("JUNK_STREAM_PAYLOAD hash verified")

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


def run_tcp_demo(adverse_case: str | None = None, adapter: TraxAdapter | None = None) -> TcpDemoResult:
    metrics = RunMetrics("tcp")
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
        target=_server, args=(listener, adapter, dag, log, metrics, server_keys), daemon=True
    )
    with metrics.measure("server.thread_start", CATEGORY_ORCHESTRATION):
        server_thread.start()

    error: str | None = None
    try:
        with metrics.measure("client.total", CATEGORY_ORCHESTRATION):
            _client(host, port, adapter, log, metrics, adverse_case, server_keys.public_key)
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

    ok = error is None and len(dag) == 2 and not any(
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
        _append_dag_output(dag, log)
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

            junk = {
                "message_type": "JUNK_STREAM_PAYLOAD",
                "session_id": session_id.hex(),
                "payload": JUNK_PAYLOAD.hex(),
                "payload_hash": payload_hash.hex(),
                "cycle_index": 0,
            }
            with metrics.measure("TRAX_REQ_ACK", CATEGORY_ORCHESTRATION):
                req_ack = _recv_tcp(sock, metrics)
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
    parser.add_argument("--runs", type=int, default=1, help="run the demo N times")
    args = parser.parse_args(argv)

    if args.runs != 1:
        results = run_repeated(run_tcp_demo, args.runs)
        if args.json:
            print_repeated_json(results)
        else:
            print_repeated_text(results)
        return 0 if all(result.ok for result in results) else 1

    result = run_tcp_demo()
    if args.json:
        print(json.dumps({"ok": result.ok, "metrics": result.metrics.as_dict()}, sort_keys=True))
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
