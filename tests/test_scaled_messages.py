from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def _assert_scaled_dag_genesis(result, messages):
    counts = result.metrics.signing_counts_summary()
    assert result.ok is True
    assert result.final_tip is not None
    assert counts["hot_path_signed_packet_count"] == 0
    assert counts["signed_genesis_create_count"] == 1
    assert counts["signed_genesis_verify_count"] == 1
    assert counts["hash_bound_message_count"] >= messages


def test_udp_demo_dag_genesis_messages_10_runs():
    result = run_udp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=10),
    )
    _assert_scaled_dag_genesis(result, 10)


def test_tcp_demo_dag_genesis_messages_10_runs():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=10),
    )
    _assert_scaled_dag_genesis(result, 10)
