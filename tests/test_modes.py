from trax_transport_lab.tcp_demo import CHECKPOINT_MODE, SIGNED_ENVELOPE_MODE, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def _assert_mode_metrics(result, mode):
    assert result.ok is True
    assert result.metrics.mode == mode
    assert result.metrics.as_dict()["mode"] == mode


def test_signed_envelope_mode_runs_for_tcp():
    result = run_tcp_demo(mode=SIGNED_ENVELOPE_MODE)
    _assert_mode_metrics(result, SIGNED_ENVELOPE_MODE)
    assert len(result.dag_nodes) == 2


def test_signed_envelope_mode_runs_for_udp():
    result = run_udp_demo(mode=SIGNED_ENVELOPE_MODE)
    _assert_mode_metrics(result, SIGNED_ENVELOPE_MODE)
    assert len(result.dag_nodes) == 2


def test_checkpoint_mode_runs_for_tcp():
    result = run_tcp_demo(mode=CHECKPOINT_MODE)
    _assert_mode_metrics(result, CHECKPOINT_MODE)
    assert len(result.dag_nodes) == 3


def test_checkpoint_mode_runs_for_udp():
    result = run_udp_demo(mode=CHECKPOINT_MODE)
    _assert_mode_metrics(result, CHECKPOINT_MODE)
    assert len(result.dag_nodes) == 3


def test_checkpoint_mode_signing_delta_for_tcp():
    signed = run_tcp_demo(mode=SIGNED_ENVELOPE_MODE)
    checkpoint = run_tcp_demo(mode=CHECKPOINT_MODE)
    signed_counts = signed.metrics.signing_counts_summary()
    checkpoint_counts = checkpoint.metrics.signing_counts_summary()

    assert checkpoint_counts["hash_bound_message_count"] > 0
    assert checkpoint_counts["signed_checkpoint_create_count"] >= 1
    assert checkpoint_counts["signed_checkpoint_verify_count"] >= 1
    assert (
        checkpoint_counts["signed_envelope_create_count"]
        < signed_counts["signed_envelope_create_count"]
    )
    assert (
        checkpoint_counts["signed_envelope_verify_count"]
        < signed_counts["signed_envelope_verify_count"]
    )
