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


def test_scale_messages_pruning_run_keeps_performance_header(capsys):
    exit_code = scale_messages.main(
        [
            "--transport",
            "udp",
            "--mode",
            "dag-genesis",
            "--counts",
            "100",
            "--runs",
            "1",
            "--max-dag-nodes",
            "50",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "avg messages_per_second:" in captured.out
    assert "avg avg_message_wall_us:" in captured.out
    assert "avg post_genesis_wall_ms:" in captured.out
    assert "avg dag_append_event_us:" in captured.out
    assert "avg dag_nodes_pruned:" in captured.out


def test_scale_messages_missing_metric_prints_unavailable(capsys):
    scale_messages._print_count(
        {
            "mode": "dag-genesis",
            "transport": "udp",
            "messages": 100000,
            "runs": 1,
            "dag_signing_cadence": 8,
            "agent_key_rotation_cadence": 0,
            "dag_key_rotation_cadence": 0,
            "key_mode": "separate",
            "max_dag_nodes": 100000,
            "ok": True,
            "summary": {},
            "warnings": [],
        }
    )
    captured = capsys.readouterr()
    assert "avg messages_per_second: unavailable" in captured.out
    assert "avg avg_message_wall_us: unavailable" in captured.out
    assert "avg post_genesis_wall_ms: unavailable" in captured.out
