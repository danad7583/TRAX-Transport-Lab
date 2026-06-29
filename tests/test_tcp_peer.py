from __future__ import annotations

import json
import socket
import threading
import time

import pytest

from trax_transport_lab.tcp_peer import (
    FRAME_REQUEST,
    PROTOCOL,
    PeerConfig,
    PeerProtocolError,
    accept_request_id,
    make_event_hash,
    read_peer_frame,
    run_peer,
    send_peer_frame,
    validate_continuity,
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def peer_configs(
    *,
    duration_seconds: float = 0.05,
    payload_size: int = 1024,
    dag_signing_cadence: int = 8,
    receiver_max_payload_size: int = 134_217_728,
):
    initiator_port = free_port()
    receiver_port = free_port()
    receiver = PeerConfig(
        node_id="node-b",
        listen_host="127.0.0.1",
        listen_port=receiver_port,
        peer_host="127.0.0.1",
        peer_port=initiator_port,
        key_mode="shared",
        duration_seconds=duration_seconds,
        payload_size=payload_size,
        safety_timeout_seconds=5,
        max_payload_size=receiver_max_payload_size,
    )
    initiator = PeerConfig(
        node_id="node-a",
        listen_host="127.0.0.1",
        listen_port=initiator_port,
        peer_host="127.0.0.1",
        peer_port=receiver_port,
        key_mode="shared",
        duration_seconds=duration_seconds,
        payload_size=payload_size,
        dag_signing_cadence=dag_signing_cadence,
        agent_key_rotation_cadence=100,
        dag_key_rotation_cadence=1000,
        safety_timeout_seconds=5,
        initiator=True,
    )
    return initiator, receiver


def run_local_session(**kwargs):
    initiator_config, receiver_config = peer_configs(**kwargs)
    receiver_result: dict = {}

    def receiver_target():
        receiver_result.update(run_peer(receiver_config))

    thread = threading.Thread(target=receiver_target, daemon=True)
    thread.start()
    time.sleep(0.05)
    initiator_result = run_peer(initiator_config)
    thread.join(timeout=5)

    assert not thread.is_alive()
    return initiator_result, receiver_result


def test_tcp_peer_valid_timed_session():
    initiator, receiver = run_local_session()

    assert initiator["ok"] is True
    assert receiver["ok"] is True
    assert initiator["final_response_received"] is True
    assert receiver["final_response_sent"] is True
    assert receiver["genesis_start_received"] is True
    assert initiator["genesis_accept_received"] is True
    assert receiver["genesis_ready_received"] is True


def test_tcp_peer_genesis_config_applied():
    initiator, receiver = run_local_session(dag_signing_cadence=3)

    assert initiator["dag_signing_cadence"] == 3
    assert receiver["dag_signing_cadence"] == 3
    assert receiver["accepted_dag_config_hash"] == initiator["dag_config_hash"]
    assert receiver["config_conflicts"]


def test_tcp_peer_genesis_accept_binds_start():
    initiator, receiver = run_local_session()

    assert receiver["accepted_genesis_hash"] == initiator["genesis_hash"]
    assert receiver["accepted_dag_config_hash"] == initiator["dag_config_hash"]
    assert receiver["genesis_accept_previous_message_hash"] == initiator["genesis_start_message_hash"]
    assert initiator["genesis_accept_previous_message_hash"] == initiator["genesis_start_message_hash"]


def test_tcp_peer_public_key_exchange():
    initiator, receiver = run_local_session()

    assert initiator["receiver_public_key_hash"]
    assert receiver["receiver_public_key_hash"] == initiator["receiver_public_key_hash"]
    assert receiver["initiator_public_key_hash"] == initiator["initiator_public_key_hash"]


def test_tcp_peer_shared_key_mode_required():
    accepted, _receiver = run_local_session()
    assert accepted["key_mode"] == "shared"

    config = PeerConfig(
        node_id="bad",
        listen_host="127.0.0.1",
        listen_port=free_port(),
        peer_host="127.0.0.1",
        peer_port=free_port(),
        key_mode="separate",
        initiator=True,
    )
    result = run_peer(config)
    assert result["ok"] is False
    assert "key-mode shared" in result["error"]


def test_tcp_peer_invalid_dag_config_rejected():
    config = PeerConfig(
        node_id="bad",
        listen_host="127.0.0.1",
        listen_port=free_port(),
        peer_host="127.0.0.1",
        peer_port=free_port(),
        key_mode="shared",
        dag_signing_cadence=0,
        initiator=True,
    )
    result = run_peer(config)
    assert result["ok"] is False
    assert "dag_signing_cadence" in result["error"]

    initiator, receiver = run_local_session(payload_size=2048, receiver_max_payload_size=1024)
    assert initiator["ok"] is False
    assert receiver["ok"] is False
    assert "payload" in receiver["error"]


def test_tcp_peer_payload_hash_mismatch_rejected():
    first, second = socket.socketpair()
    try:
        header = {
            "type": FRAME_REQUEST,
            "protocol": PROTOCOL,
            "payload_size": 5,
            "payload_hash": "0" * 64,
        }
        send_peer_frame(first, header, b"hello")
        with pytest.raises(PeerProtocolError, match="payload hash mismatch"):
            read_peer_frame(second)
    finally:
        first.close()
        second.close()


def test_tcp_peer_duplicate_request_id_rejected():
    seen_request_ids: set[int] = set()
    accept_request_id(seen_request_ids, 7)

    with pytest.raises(PeerProtocolError, match="duplicate request_id"):
        accept_request_id(seen_request_ids, 7)


def test_tcp_peer_invalid_continuity_rejected():
    metrics = {"final_tip": "abc"}
    header = {
        "type": FRAME_REQUEST,
        "session_id": "s",
        "run_id": "r",
        "request_id": 1,
        "payload_hash": "p",
        "previous_tip": "wrong",
    }
    header["event_hash"] = make_event_hash(FRAME_REQUEST, header)

    with pytest.raises(PeerProtocolError, match="invalid previous_tip"):
        validate_continuity(metrics, FRAME_REQUEST, header)

    header["previous_tip"] = "abc"
    header["event_hash"] = "wrong"
    with pytest.raises(PeerProtocolError, match="invalid event_hash"):
        validate_continuity(metrics, FRAME_REQUEST, header)


def test_tcp_peer_traffic_stop_does_not_close_dag():
    initiator, receiver = run_local_session()

    assert initiator["traffic_stop_sent"] is True
    assert receiver["traffic_stop_received"] is True
    assert initiator["dag_finalized"] is False
    assert receiver["dag_finalized"] is False
    assert initiator["dag_closed"] is False
    assert receiver["dag_closed"] is False


def test_tcp_peer_json_output_parseable(capsys):
    from trax_transport_lab.tcp_peer import main

    config_port = free_port()
    result = main(
        [
            "--node-id",
            "bad",
            "--listen-host",
            "127.0.0.1",
            "--listen-port",
            str(config_port),
            "--peer-host",
            "127.0.0.1",
            "--peer-port",
            str(free_port()),
            "--key-mode",
            "separate",
            "--initiator",
            "--json",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert result == 1
    assert output["ok"] is False
    assert output["key_mode"] == "separate"
