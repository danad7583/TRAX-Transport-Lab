from trax_transport_lab.tcp_demo import run_tcp_demo


def test_happy_path_tcp_demo():
    result = run_tcp_demo()
    assert result.ok is True
    assert result.transport == "tcp"
    assert len(result.dag_nodes) == 2
    assert result.dag_nodes[0].node_type == "SESSION_START_V0"
    assert result.dag_nodes[1].node_type == "STREAM_EXCHANGE_V0"
    assert result.final_tip is not None
    assert "JUNK_STREAM_PAYLOAD hash verified" in result.log_lines
    assert result.metrics.transport == "tcp"
    assert result.metrics.dag_nodes_appended == 2
    assert result.metrics.total_bytes_sent > 0
    assert result.metrics.total_bytes_received > 0
    assert result.metrics.frames_sent > 0
    assert result.metrics.frames_received > 0
    assert "payload_hash_verify" in result.metrics.event_names()
    buckets = result.metrics.bucket_summary()
    assert "trax_primitives_event_ms" in buckets
    assert "python_packaging_event_ms" in buckets
    assert "transport_io_event_ms" in buckets
    assert "dag_event_ms" in buckets
    assert result.metrics.micro_highlights()["payload_hash_verify_us"] >= 0
