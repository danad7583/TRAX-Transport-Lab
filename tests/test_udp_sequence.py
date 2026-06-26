from trax_transport_lab.udp_demo import run_udp_demo


def test_happy_path_udp_demo():
    result = run_udp_demo()
    assert result.ok is True
    assert result.transport == "udp"
    assert len(result.dag_nodes) == 2
    assert result.dag_nodes[0].node_type == "SESSION_START_V0"
    assert result.dag_nodes[1].node_type == "STREAM_EXCHANGE_V0"
    assert result.final_tip is not None
    assert "JUNK_STREAM_PAYLOAD hash verified" in result.log_lines
    assert result.metrics.transport == "udp"
    assert result.metrics.dag_nodes_appended == 2
    assert result.metrics.total_bytes_sent > 0
    assert result.metrics.total_bytes_received > 0
    assert result.metrics.datagrams_sent > 0
    assert result.metrics.datagrams_received > 0
    assert "payload_hash_verify" in result.metrics.event_names()
    buckets = result.metrics.bucket_summary()
    assert "trax_primitives_ms" in buckets
    assert "python_packaging_ms" in buckets
    assert "transport_io_ms" in buckets
    assert "dag_ms" in buckets
    assert result.metrics.key_event_summary()["payload_hash_verify_us"] >= 0
