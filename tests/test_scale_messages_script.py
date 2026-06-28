import json

from scripts import scale_messages


def test_scale_messages_udp_dag_genesis_runs(capsys):
    exit_code = scale_messages.main(
        ["--transport", "udp", "--mode", "dag-genesis", "--counts", "10", "--runs", "1"]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "TRAX scaled message comparison" in captured.out


def test_scale_messages_json_is_parseable(capsys):
    exit_code = scale_messages.main(
        [
            "--transport",
            "udp",
            "--mode",
            "dag-genesis",
            "--counts",
            "10",
            "--runs",
            "1",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    count_payload = payload["transports"]["udp"]["10"]
    assert exit_code == 0
    for name in [
        "messages_per_second",
        "avg_message_wall_us",
        "dag_segment_count",
        "agent_key_rotation_event_count",
        "dag_key_rotation_event_count",
    ]:
        assert name in count_payload["summary"]
