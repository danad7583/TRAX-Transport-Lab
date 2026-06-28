from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo


def test_messages_100_agent_rotation_100_rotates_once():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=100, agent_key_rotation_cadence=100),
    )
    assert result.metrics.agent_key_rotation_event_count == 1
    assert result.metrics.agent_key_rotation_signed_packet_count == 1
    assert result.metrics.hot_path_signed_packet_count == 0


def test_messages_1000_agent_rotation_100_rotates_ten_times():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=1000, agent_key_rotation_cadence=100),
    )
    assert result.metrics.agent_key_rotation_event_count == 10
    assert result.metrics.agent_key_rotation_signed_packet_count == 10


def test_key_rotation_cadence_alias_maps_to_agent_rotation():
    config = make_scale_config(messages=100, key_rotation_cadence_alias_value=100)
    result = run_tcp_demo(mode=DAG_GENESIS_MODE, scale_config=config)
    assert result.metrics.agent_key_rotation_cadence == 100
    assert result.metrics.key_rotation_cadence_alias_value == 100
    assert result.metrics.agent_key_rotation_event_count == 1


def test_agent_rotation_zero_rotates_zero_times():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=100, agent_key_rotation_cadence=0),
    )
    assert result.metrics.agent_key_rotation_event_count == 0
    assert result.metrics.agent_key_rotation_signed_packet_count == 0
