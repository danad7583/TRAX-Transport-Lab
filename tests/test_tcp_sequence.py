from trax_transport_lab.tcp_demo import run_tcp_demo


def test_happy_path_tcp_demo():
    result = run_tcp_demo()
    assert result.ok is True
    assert len(result.dag_nodes) == 2
    assert result.dag_nodes[0].node_type == "SESSION_START_V0"
    assert result.dag_nodes[1].node_type == "STREAM_EXCHANGE_V0"
    assert result.final_tip is not None
    assert "JUNK_STREAM_PAYLOAD hash verified" in result.log_lines
