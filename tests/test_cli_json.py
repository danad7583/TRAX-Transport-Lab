import json

from trax_transport_lab.tcp_demo import main as tcp_main
from trax_transport_lab.udp_demo import main as udp_main


def test_tcp_json_emits_parseable_json_only(capsys):
    exit_code = tcp_main(["--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["transport"] == "tcp"
    assert "wall_clock" in payload
    assert "event_sums" in payload
    assert "events" not in payload
    assert "events_by_category" not in payload
    assert "total_duration_ms" not in payload
    assert captured.out.lstrip().startswith("{")


def test_udp_json_emits_parseable_json_only(capsys):
    exit_code = udp_main(["--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["transport"] == "udp"
    assert "wall_clock" in payload
    assert "event_sums" in payload
    assert "events" not in payload
    assert "events_by_category" not in payload
    assert "total_duration_ms" not in payload
    assert captured.out.lstrip().startswith("{")


def test_tcp_json_include_events_includes_raw_events(capsys):
    exit_code = tcp_main(["--json", "--include-events"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert "events" in payload
    assert "events_by_category" in payload


def test_udp_json_include_events_includes_raw_events(capsys):
    exit_code = udp_main(["--json", "--include-events"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert "events" in payload
    assert "events_by_category" in payload
