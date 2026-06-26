import json

from trax_transport_lab.tcp_demo import main as tcp_main
from trax_transport_lab.udp_demo import main as udp_main


def test_tcp_json_emits_parseable_json_only(capsys):
    exit_code = tcp_main(["--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["metrics"]["transport"] == "tcp"
    assert "wall_clock" in payload["metrics"]
    assert "event_sums" in payload["metrics"]
    assert "total_duration_ms" not in payload["metrics"]
    assert captured.out.lstrip().startswith("{")


def test_udp_json_emits_parseable_json_only(capsys):
    exit_code = udp_main(["--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["metrics"]["transport"] == "udp"
    assert "wall_clock" in payload["metrics"]
    assert "event_sums" in payload["metrics"]
    assert "total_duration_ms" not in payload["metrics"]
    assert captured.out.lstrip().startswith("{")
