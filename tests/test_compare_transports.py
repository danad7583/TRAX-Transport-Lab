from scripts.compare_transports import comparison_payload, print_text
from trax_transport_lab.tcp_demo import run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def test_compare_transports_output_uses_clear_timing_terms(capsys):
    payload = comparison_payload([run_tcp_demo()], [run_udp_demo()])
    print_text(payload)
    captured = capsys.readouterr()
    assert "Wall-clock averages" in captured.out
    assert "Event-sum averages, may overlap" in captured.out
    assert "Primitive highlights" in captured.out
    assert "total_wall_ms" in captured.out
    assert "transport_io_event_ms" in captured.out


def test_compare_json_compact_by_default():
    payload = comparison_payload([run_tcp_demo()], [run_udp_demo()])
    assert "raw_runs" not in payload
    assert "interpretation" in payload


def test_compare_json_include_events_has_raw_runs():
    payload = comparison_payload([run_tcp_demo()], [run_udp_demo()], include_events=True)
    assert "raw_runs" in payload
    assert "events" in payload["raw_runs"]["tcp"][0]["metrics"]
