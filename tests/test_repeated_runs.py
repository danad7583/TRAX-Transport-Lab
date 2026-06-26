from trax_transport_lab.tcp_demo import run_tcp_demo
from trax_transport_lab.transport_common import repeated_result_payload, run_repeated
from trax_transport_lab.udp_demo import run_udp_demo


def test_tcp_repeated_run_aggregate_runs_twice():
    results = run_repeated(run_tcp_demo, 2)
    payload = repeated_result_payload(results)
    assert payload["ok"] is True
    assert payload["runs"] == 2
    assert "total_duration_ms" in payload["aggregate"]


def test_udp_repeated_run_aggregate_runs_twice():
    results = run_repeated(run_udp_demo, 2)
    payload = repeated_result_payload(results)
    assert payload["ok"] is True
    assert payload["runs"] == 2
    assert "transport_io_ms" in payload["aggregate"]
