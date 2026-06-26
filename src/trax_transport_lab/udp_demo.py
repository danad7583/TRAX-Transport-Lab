from __future__ import annotations

import argparse
import json
import socket
import threading
from time import perf_counter_ns
from typing import Any

from .dag_model import DemoDag
from .logging_utils import DemoLog
from .messages import MessageError, decode_message, encode_message, hex_to_bytes
from .metrics import CATEGORY_ORCHESTRATION, CATEGORY_TRANSPORT_IO, CATEGORY_TRAX, RunMetrics
from .tcp_demo import (
    INIT_SESSION_ID_SEED,
    ProtocolError,
    _append_dag_output,
    _make_security_message,
    _packet_hash,
    _validate_security_message,
    security_payload,
)
from .trax_adapter import TraxAdapter
from .transport_common import TransportDemoResult
from .transport_common import print_repeated_json, print_repeated_text, run_repeated


UDP_PAYLOAD = b"TRAX_UDP_TEST_BLOCK_0001" * 32
UDP_MAX_DATAGRAM = 64 * 1024
UDP_TIMEOUT_SECONDS = 2.0

UdpDemoResult = TransportDemoResult


def _send_udp(
    sock: socket.socket,
    message: dict[str, Any],
    address,
    metrics: RunMetrics,
) -> None:
    data = encode_message(message, metrics=metrics)
    if len(data) > UDP_MAX_DATAGRAM:
        raise ProtocolError(message.get("message_type", "<unknown>"), "datagram too large")
    with metrics.measure("udp.send_datagram", CATEGORY_TRANSPORT_IO):
        sent = sock.sendto(data, address)
        metrics.add_datagram_sent()
        metrics.add_bytes_sent(sent)


def _recv_udp(sock: socket.socket, metrics: RunMetrics) -> tuple[dict[str, Any], tuple[str, int]]:
    with metrics.measure("udp.recv_datagram", CATEGORY_TRANSPORT_IO):
        data, address = sock.recvfrom(UDP_MAX_DATAGRAM)
        metrics.add_datagram_received()
        metrics.add_bytes_received(len(data))
    return decode_message(data, metrics=metrics), address


def _server(
    sock: socket.socket,
    adapter: TraxAdapter,
    dag: DemoDag,
    log: DemoLog,
    metrics: RunMetrics,
    server_keys,
) -> None:
    client_address = None
    server_started_ns = perf_counter_ns()
    try:
        with metrics.measure("server.recv_init", CATEGORY_ORCHESTRATION):
            init_session_id = adapter.hash32(INIT_SESSION_ID_SEED)

            with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                init, client_address = _recv_udp(sock, metrics)
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
            _send_udp(sock, init_ack, client_address, metrics)
        log.add("TRAX_INIT_ACK sent")

        with metrics.measure("TRAX_COMMIT", CATEGORY_ORCHESTRATION):
            commit, _ = _recv_udp(sock, metrics)
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
            req, _ = _recv_udp(sock, metrics)
        committed_payload_hash = hex_to_bytes(req["payload_hash"], "payload_hash", metrics=metrics)
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
            _send_udp(sock, req_ack, client_address, metrics)
        log.add("TRAX_REQ_ACK sent")

        with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
            junk, _ = _recv_udp(sock, metrics)
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
            _send_udp(sock, res_ack, client_address, metrics)
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
    except socket.timeout as exc:
        log.reject("<timeout>", str(exc) or "udp receive timed out")
    except ProtocolError as exc:
        log.reject(exc.message_type, exc.reason)
    except (MessageError, OSError, KeyError, ValueError) as exc:
        log.reject("<unknown>", str(exc))
    finally:
        server_ended_ns = perf_counter_ns()
        metrics.record_event(
            "server.total",
            CATEGORY_ORCHESTRATION,
            server_started_ns,
            server_ended_ns,
        )
        sock.close()


def run_udp_demo(adverse_case: str | None = None, adapter: TraxAdapter | None = None) -> UdpDemoResult:
    metrics = RunMetrics("udp")
    demo_started_ns = perf_counter_ns()
    adapter = adapter or TraxAdapter(metrics=metrics)
    adapter.metrics = metrics
    dag = DemoDag(metrics=metrics)
    log = DemoLog()
    with metrics.measure("demo.thread_startup", CATEGORY_ORCHESTRATION):
        server_keys = adapter.generate_keypair()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.settimeout(UDP_TIMEOUT_SECONDS)
    server_address = server_sock.getsockname()

    server_thread = threading.Thread(
        target=_server,
        args=(server_sock, adapter, dag, log, metrics, server_keys),
        daemon=True,
    )
    with metrics.measure("server.thread_start", CATEGORY_ORCHESTRATION):
        server_thread.start()

    error: str | None = None
    try:
        with metrics.measure("client.total", CATEGORY_ORCHESTRATION):
            _client(server_address, adapter, log, metrics, adverse_case, server_keys.public_key)
    except ProtocolError as exc:
        log.reject(exc.message_type, exc.reason)
        error = exc.reason
    except (MessageError, OSError, KeyError, ValueError) as exc:
        log.reject("<client>", str(exc))
        error = str(exc)

    with metrics.measure("server.thread_join", CATEGORY_ORCHESTRATION):
        server_thread.join(UDP_TIMEOUT_SECONDS + 1.0)
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
        log.add("")
        log.add("Final tip:")
        log.add(dag.final_tip().hex() if dag.final_tip() else "<none>")
        log.add("")
        log.lines.extend(metrics.summary_lines())

    return UdpDemoResult(
        ok=ok,
        transport="udp",
        dag_nodes=dag.enumerate(),
        final_tip=dag.final_tip(),
        log_lines=list(log.lines),
        metrics=metrics,
        error=error,
    )


def _client(
    server_address,
    adapter: TraxAdapter,
    log: DemoLog,
    metrics: RunMetrics,
    adverse_case: str | None,
    observed_server_public_key: bytes,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(UDP_TIMEOUT_SECONDS)
        client_keys = adapter.generate_keypair()
        init_session_id = adapter.hash32(INIT_SESSION_ID_SEED)
        client_nonce = adapter.generate_nonce()

        if adverse_case == "timeout":
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
            with metrics.measure("udp.send_datagram", CATEGORY_TRANSPORT_IO):
                sent = sock.sendto(b"not-json!", server_address)
                metrics.add_datagram_sent()
                metrics.add_bytes_sent(sent)
            log.add("TRAX_INIT sent")
            return

        with metrics.measure("session_handshake_total", CATEGORY_ORCHESTRATION):
            with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                _send_udp(sock, init, server_address, metrics)
            log.add("TRAX_INIT sent")

            if adverse_case == "duplicate_init":
                with metrics.measure("TRAX_INIT", CATEGORY_ORCHESTRATION):
                    _send_udp(sock, init, server_address, metrics)
                log.add("TRAX_INIT sent")

            if adverse_case == "req_before_commit":
                payload_hash = adapter.hash32(UDP_PAYLOAD)
                bad_req_payload = security_payload(
                    "TRAX_REQ",
                    {
                        "cycle_index": 0,
                        "payload_hash": payload_hash.hex(),
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
                    payload_hash=payload_hash.hex(),
                )
                with metrics.measure("TRAX_REQ", CATEGORY_ORCHESTRATION):
                    _send_udp(sock, bad_req, server_address, metrics)
                log.add("TRAX_REQ sent")
                return

            with metrics.measure("TRAX_INIT_ACK", CATEGORY_ORCHESTRATION):
                init_ack, _ = _recv_udp(sock, metrics)
            server_public_key = hex_to_bytes(init_ack["sender_public_key"], "sender_public_key", metrics=metrics)
            server_nonce = hex_to_bytes(init_ack["server_nonce"], "server_nonce", metrics=metrics)
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
                commit_session_id = adapter.hash32(b"wrong-udp-session")
            commit = _make_security_message(
                adapter,
                "TRAX_COMMIT",
                client_keys.private_key,
                client_keys.public_key,
                server_public_key,
                commit_session_id,
                commit_payload,
            )
            with metrics.measure("TRAX_COMMIT", CATEGORY_ORCHESTRATION):
                _send_udp(sock, commit, server_address, metrics)
            log.add("TRAX_COMMIT sent")
            if adverse_case == "wrong_session":
                return

        payload_hash = adapter.hash32(UDP_PAYLOAD)
        if adverse_case == "payload_before_ack":
            junk = {
                "message_type": "JUNK_STREAM_PAYLOAD",
                "session_id": session_id.hex(),
                "payload": UDP_PAYLOAD.hex(),
                "payload_hash": payload_hash.hex(),
                "cycle_index": 0,
            }
            with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
                _send_udp(sock, junk, server_address, metrics)
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
                _send_udp(sock, req, server_address, metrics)
            log.add("TRAX_REQ sent")
            if adverse_case == "wrong_message_order":
                return

            junk = {
                "message_type": "JUNK_STREAM_PAYLOAD",
                "session_id": session_id.hex(),
                "payload": UDP_PAYLOAD.hex(),
                "payload_hash": payload_hash.hex(),
                "cycle_index": 0,
            }
            with metrics.measure("TRAX_REQ_ACK", CATEGORY_ORCHESTRATION):
                req_ack, _ = _recv_udp(sock, metrics)
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
                junk["payload"] = (UDP_PAYLOAD + b"tampered").hex()
            with metrics.measure("JUNK_STREAM_PAYLOAD", CATEGORY_ORCHESTRATION):
                _send_udp(sock, junk, server_address, metrics)
            log.add("JUNK_STREAM_PAYLOAD sent")
            if adverse_case == "payload_hash_mismatch":
                return

            with metrics.measure("TRAX_RES_ACK", CATEGORY_ORCHESTRATION):
                res_ack, _ = _recv_udp(sock, metrics)
            res_payload = security_payload(
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
                res_payload,
                session_id,
            )
            log.add("TRAX_RES_ACK accepted")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the TRAX UDP transport demo.")
    parser.add_argument("--json", action="store_true", help="emit result metrics as JSON")
    parser.add_argument("--include-events", action="store_true", help="include raw metric events in JSON")
    parser.add_argument("--runs", type=int, default=1, help="run the demo N times")
    args = parser.parse_args(argv)

    if args.runs != 1:
        results = run_repeated(run_udp_demo, args.runs)
        if args.json:
            print_repeated_json(results, include_events=args.include_events)
        else:
            print_repeated_text(results)
        return 0 if all(result.ok for result in results) else 1

    result = run_udp_demo()
    if args.json:
        payload = {
            "ok": result.ok,
            **result.metrics.as_dict(include_events=args.include_events),
        }
        print(json.dumps(payload, sort_keys=True))
        return 0 if result.ok else 1

    if result.ok:
        print("UDP DEMO OK")
        print()
    else:
        print("UDP DEMO FAILED")
        print()
    for line in result.log_lines:
        print(line)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
