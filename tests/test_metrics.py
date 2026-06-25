from trax_transport_lab.metrics import RunMetrics


def test_run_metrics_total_duration_and_json():
    metrics = RunMetrics("tcp")
    with metrics.measure("TRAX_INIT") as event:
        event.add_sent(10)
        event.add_received(20)
    metrics.add_frame_sent()
    metrics.add_frame_received()
    metrics.add_dag_node(bytes.fromhex("aa" * 32))
    metrics.finish(bytes.fromhex("bb" * 32))

    assert metrics.total_duration_ns() >= 0
    assert metrics.events[0].duration_ns >= 0
    assert '"transport":"tcp"' in metrics.to_json()
    assert metrics.as_dict()["total_bytes_sent"] == 10
    assert metrics.as_dict()["total_bytes_received"] == 20


def test_summary_lines_include_key_fields():
    metrics = RunMetrics("udp")
    metrics.add_datagram_sent()
    metrics.add_datagram_received()
    metrics.add_bytes_sent(100)
    metrics.add_bytes_received(200)
    metrics.add_dag_node(bytes.fromhex("11" * 32))
    metrics.finish(bytes.fromhex("22" * 32))

    lines = metrics.summary_lines()
    joined = "\n".join(lines)
    assert "transport: udp" in joined
    assert "total_duration_ms:" in joined
    assert "bytes_sent: 100" in joined
    assert "bytes_received: 200" in joined
    assert "dag_nodes_appended: 1" in joined
    assert f"final_tip: {bytes.fromhex('22' * 32).hex()}" in joined
