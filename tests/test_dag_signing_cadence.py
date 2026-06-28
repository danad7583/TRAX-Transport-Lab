from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo


def _run(messages, cadence, seal_final_partial=False):
    return run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(
            messages=messages,
            dag_signing_cadence=cadence,
            seal_final_partial=seal_final_partial,
        ),
    )


def test_messages_10_cadence_8_seals_one_complete_segment():
    assert _run(10, 8).metrics.dag_segment_count == 1


def test_messages_10_cadence_8_can_seal_final_partial():
    assert _run(10, 8, seal_final_partial=True).metrics.dag_segment_count == 2


def test_messages_100_cadence_8_seals_twelve_segments():
    assert _run(100, 8).metrics.dag_segment_count == 12


def test_messages_100_cadence_10_seals_ten_segments():
    assert _run(100, 10).metrics.dag_segment_count == 10
