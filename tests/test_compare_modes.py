import json

from scripts import compare_modes


def test_compare_modes_runs(capsys):
    exit_code = compare_modes.main(["--runs", "1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "TRAX mode comparison" in captured.out
    assert "Delta:" in captured.out
    assert "not benchmark-grade" in captured.out


def test_compare_modes_udp_runs(capsys):
    exit_code = compare_modes.main(["--transport", "udp", "--runs", "1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "transport: udp" in captured.out
    assert "transport: tcp" not in captured.out


def test_compare_modes_tcp_runs(capsys):
    exit_code = compare_modes.main(["--transport", "tcp", "--runs", "1"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "transport: tcp" in captured.out
    assert "transport: udp" not in captured.out


def test_compare_modes_json_parseable(capsys):
    exit_code = compare_modes.main(["--json", "--runs", "1"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert "tcp" in payload["transports"]
    assert "udp" in payload["transports"]
    assert "delta" in payload["transports"]["tcp"]
