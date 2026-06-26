import json

from trax_transport_lab.tcp_demo import CHECKPOINT_MODE, SIGNED_ENVELOPE_MODE
from trax_transport_lab.tcp_demo import main as tcp_main
from trax_transport_lab.udp_demo import main as udp_main


def _assert_cli_mode(main, mode, transport, capsys):
    exit_code = main(["--mode", mode, "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["transport"] == transport
    assert payload["mode"] == mode
    assert "signing_counts" in payload
    assert "events" not in payload
    return payload


def test_tcp_demo_signed_envelope_mode_json(capsys):
    _assert_cli_mode(tcp_main, SIGNED_ENVELOPE_MODE, "tcp", capsys)


def test_tcp_demo_checkpoint_mode_json(capsys):
    payload = _assert_cli_mode(tcp_main, CHECKPOINT_MODE, "tcp", capsys)
    assert payload["signing_counts"]["hash_bound_message_count"] > 0


def test_udp_demo_signed_envelope_mode_json(capsys):
    _assert_cli_mode(udp_main, SIGNED_ENVELOPE_MODE, "udp", capsys)


def test_udp_demo_checkpoint_mode_json(capsys):
    payload = _assert_cli_mode(udp_main, CHECKPOINT_MODE, "udp", capsys)
    assert payload["signing_counts"]["signed_checkpoint_verify_count"] >= 1
