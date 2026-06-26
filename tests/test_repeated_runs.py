from trax_transport_lab.tcp_demo import run_tcp_demo
from trax_transport_lab.transport_common import (
    print_repeated_json,
    print_repeated_text,
    repeated_result_payload,
    run_repeated,
)
from trax_transport_lab.udp_demo import run_udp_demo


def test_tcp_repeated_run_aggregate_runs_twice():
    results = run_repeated(run_tcp_demo, 2)
    payload = repeated_result_payload(results)
    assert payload["ok"] is True
    assert payload["runs"] == 2
    assert "total_wall_ms" in payload["aggregate"]
    assert "trax_primitives_event_ms" in payload["aggregate"]
    assert "events" not in payload["per_run"][0]["metrics"]


def test_udp_repeated_run_aggregate_runs_twice():
    results = run_repeated(run_udp_demo, 2)
    payload = repeated_result_payload(results)
    assert payload["ok"] is True
    assert payload["runs"] == 2
    assert "transport_io_event_ms" in payload["aggregate"]


def test_repeated_text_output_is_compact(capsys):
    results = run_repeated(run_udp_demo, 2)
    print_repeated_text(results)
    captured = capsys.readouterr()
    assert "Primitive highlights:" in captured.out
    assert "Event sums, may overlap:" in captured.out
    assert "events_by_category" not in captured.out


def test_repeated_json_compact_output_parses(capsys):
    results = run_repeated(run_tcp_demo, 2)
    print_repeated_json(results)
    captured = capsys.readouterr()
    import json
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert "events" not in payload["per_run"][0]["metrics"]
