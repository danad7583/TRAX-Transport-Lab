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
    assert metrics.as_dict()["counts"]["bytes_sent"] == 10
    assert metrics.as_dict()["counts"]["bytes_received"] == 20
    assert set(CATEGORIES).issubset(metrics.events_by_category())
    assert metrics.category_duration_ns(CATEGORY_TRAX) >= 0
    assert metrics.category_duration_ms(CATEGORY_PYTHON_PACKAGING) >= 0
    assert metrics.named_event_total_ns("trax.hash32") >= 0
    assert metrics.named_event_total_us("trax.hash32") >= 0
    assert "wall_clock" in metrics.as_dict()
    assert "event_sums" in metrics.as_dict()
    assert "micro_highlights" in metrics.as_dict()
    assert "counts" in metrics.as_dict()
    assert "total_wall_ms" in metrics.wall_clock_summary()
    assert "trax_primitives_event_ms" in metrics.event_sum_summary()
    assert "payload_hash_verify_us" in metrics.micro_highlights()
    assert "events" not in metrics.compact_dict()
    assert "events_by_category" not in metrics.compact_dict()
    assert "events" in metrics.full_dict()
    assert "events_by_category" in metrics.full_dict()


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
    assert "bytes_sent: 100" in joined
    assert "bytes_received: 200" in joined
    assert "dag_nodes_appended: 1" in joined
    assert "Wall-clock:" in joined
    assert "Event sums, may overlap:" in joined
    assert "total_wall_ms:" in joined
    assert "trax_primitives_event_ms:" in joined
    assert "Primitive highlights:" in joined
    assert "payload_hash_verify_us:" in joined
    assert "Slowest events:" in joined


def test_slowest_events_sorted_and_limited():
    metrics = RunMetrics("tcp")
    metrics.record_event("slow", CATEGORY_TRAX, 0, 3000)
    metrics.record_event("fast", CATEGORY_DAG, 0, 1000)
    events = metrics.slowest_events(limit=1)
    assert len(events) == 1
    assert events[0]["name"] == "slow"
    assert events[0]["category"] == CATEGORY_TRAX
    assert events[0]["duration_us"] == 3.0
