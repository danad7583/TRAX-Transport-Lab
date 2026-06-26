from trax_transport_lab.metrics import (
    CATEGORY_DAG,
    CATEGORY_PYTHON_PACKAGING,
    CATEGORY_TRAX,
    CATEGORIES,
    RunMetrics,
)


def test_run_metrics_total_duration_and_json():
    metrics = RunMetrics("tcp")
    with metrics.measure("TRAX_INIT", CATEGORY_PYTHON_PACKAGING) as event:
        event.add_sent(10)
        event.add_received(20)
    with metrics.measure("trax.hash32", CATEGORY_TRAX):
        pass
    metrics.add_frame_sent()
    metrics.add_frame_received()
    metrics.add_dag_node(bytes.fromhex("aa" * 32))
    metrics.finish(bytes.fromhex("bb" * 32))

    assert metrics.total_duration_ns() >= 0
    assert metrics.events[0].duration_ns >= 0
    assert '"transport":"tcp"' in metrics.to_json()
    assert metrics.as_dict()["total_bytes_sent"] == 10
    assert metrics.as_dict()["total_bytes_received"] == 20
    assert set(CATEGORIES).issubset(metrics.events_by_category())
    assert metrics.category_duration_ns(CATEGORY_TRAX) >= 0
    assert metrics.category_duration_ms(CATEGORY_PYTHON_PACKAGING) >= 0
    assert metrics.named_event_total_ns("trax.hash32") >= 0
    assert metrics.named_event_total_us("trax.hash32") >= 0
    assert "bucket_summary" in metrics.as_dict()
    assert "key_event_summary" in metrics.as_dict()


def test_summary_lines_include_key_fields():
    metrics = RunMetrics("udp")
    with metrics.measure("dag.append_node", CATEGORY_DAG):
        pass
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
    assert "Buckets:" in joined
    assert "trax_primitives_ms:" in joined
    assert "Key Events:" in joined
    assert "payload_hash_verify_us:" in joined
