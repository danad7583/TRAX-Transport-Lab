import json

from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE
from trax_transport_lab.tcp_demo import main as tcp_main
from trax_transport_lab.udp_demo import main as udp_main


def _run_json(main, include_events, capsys):
    args = ["--mode", DAG_GENESIS_MODE, "--json"]
    if include_events:
        args.append("--include-events")
    exit_code = main(args)
    captured = capsys.readouterr()
    return exit_code, json.loads(captured.out)


def test_tcp_demo_dag_genesis_json_is_compact(capsys):
    exit_code, payload = _run_json(tcp_main, False, capsys)
    assert exit_code == 0
    assert payload["mode"] == DAG_GENESIS_MODE
    assert payload["signing_counts"]["hot_path_signed_packet_count"] == 0
    assert "events" not in payload


def test_udp_demo_dag_genesis_json_is_compact(capsys):
    exit_code, payload = _run_json(udp_main, False, capsys)
    assert exit_code == 0
    assert payload["mode"] == DAG_GENESIS_MODE
    assert payload["signing_counts"]["signed_genesis_create_count"] == 1
    assert "events_by_category" not in payload


def test_tcp_demo_dag_genesis_include_events(capsys):
    exit_code, payload = _run_json(tcp_main, True, capsys)
    assert exit_code == 0
    assert "events" in payload
    assert "events_by_category" in payload
