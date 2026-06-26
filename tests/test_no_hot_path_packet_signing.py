from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, SIGNED_ENVELOPE_MODE, run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def test_dag_genesis_mode_does_not_count_packet_envelope_creation():
    result = run_tcp_demo(mode=DAG_GENESIS_MODE)
    counts = result.metrics.signing_counts_summary()
    assert counts["signed_envelope_create_count"] == 0
    assert counts["signed_envelope_verify_count"] == 0
    assert counts["hot_path_signed_packet_count"] == 0
    assert "signed_envelope.create" not in result.metrics.event_names()
    assert "signed_envelope.verify" not in result.metrics.event_names()


def test_dag_genesis_mode_uses_genesis_signature_only_for_udp():
    result = run_udp_demo(mode=DAG_GENESIS_MODE)
    counts = result.metrics.signing_counts_summary()
    assert counts["signed_genesis_create_count"] == 1
    assert counts["signed_genesis_verify_count"] == 1
    assert counts["hot_path_signed_packet_count"] == 0


def test_signed_envelope_mode_still_counts_packet_envelopes():
    result = run_tcp_demo(mode=SIGNED_ENVELOPE_MODE)
    counts = result.metrics.signing_counts_summary()
    assert counts["signed_envelope_create_count"] > 0
    assert counts["signed_envelope_verify_count"] > 0
    assert counts["hot_path_signed_packet_count"] > 0
