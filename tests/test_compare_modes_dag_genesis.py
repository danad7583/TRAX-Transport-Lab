import json

from scripts import compare_modes
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, SIGNED_ENVELOPE_MODE


def test_compare_modes_includes_dag_genesis(capsys):
    exit_code = compare_modes.main(["--transport", "udp", "--runs", "1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "dag-genesis mode" in captured.out
    assert "hot_path_signed_packet_count_delta" in captured.out


def test_compare_modes_signed_envelope_vs_dag_genesis_runs(capsys):
    exit_code = compare_modes.main(
        [
            "--mode-a",
            SIGNED_ENVELOPE_MODE,
            "--mode-b",
            DAG_GENESIS_MODE,
            "--transport",
            "udp",
            "--runs",
            "1",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "signed_envelope_event_ms_delta" in captured.out


def test_compare_modes_dag_genesis_json_parseable(capsys):
    exit_code = compare_modes.main(["--json", "--runs", "1"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["mode_b"] == DAG_GENESIS_MODE
    assert "hot_path_signed_packet_count_delta" in payload["transports"]["tcp"]["delta"]
    assert "signed_envelope_event_ms_delta" in payload["transports"]["udp"]["delta"]
