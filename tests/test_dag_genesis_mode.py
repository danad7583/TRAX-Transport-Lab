from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def _assert_dag_genesis_result(result):
    counts = result.metrics.signing_counts_summary()
    assert result.ok is True
    assert result.metrics.mode == DAG_GENESIS_MODE
    assert result.metrics.as_dict()["mode"] == DAG_GENESIS_MODE
    assert counts["signed_genesis_create_count"] == 1
    assert counts["signed_genesis_verify_count"] == 1
    assert counts["hot_path_signed_packet_count"] == 0
    assert counts["signed_envelope_create_count"] == 0
    assert counts["signed_envelope_verify_count"] == 0
    assert counts["hash_bound_message_count"] > 0
    assert result.final_tip is not None
    assert "payload_hash_verify" in result.metrics.event_names()
    assert result.dag_nodes[0].node_type == "SESSION_START_V0"
    assert result.dag_nodes[1].node_type == "STREAM_EXCHANGE_V0"


def test_dag_genesis_mode_runs_for_tcp():
    _assert_dag_genesis_result(run_tcp_demo(mode=DAG_GENESIS_MODE))


def test_dag_genesis_mode_runs_for_udp():
    _assert_dag_genesis_result(run_udp_demo(mode=DAG_GENESIS_MODE))
