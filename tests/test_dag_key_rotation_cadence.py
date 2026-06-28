from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo


def test_messages_100_dag_key_rotation_100_rotates_once():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=100, dag_key_rotation_cadence=100),
    )
    assert result.metrics.dag_key_rotation_event_count == 1
    assert result.metrics.agent_key_rotation_event_count == 0
    assert result.metrics.hot_path_signed_packet_count == 0


def test_dag_key_rotation_zero_rotates_zero_times():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=100, dag_key_rotation_cadence=0),
    )
    assert result.metrics.dag_key_rotation_event_count == 0
