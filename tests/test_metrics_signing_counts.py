from trax_transport_lab.metrics import CATEGORY_TRAX, RunMetrics


def test_run_metrics_counts_signed_envelope_events():
    metrics = RunMetrics("tcp", mode="checkpoint")
    with metrics.measure("signed_envelope.create", CATEGORY_TRAX):
        metrics.add_signed_envelope_create()
    with metrics.measure("signed_envelope.verify", CATEGORY_TRAX):
        metrics.add_signed_envelope_verify()
    with metrics.measure("checkpoint.create_signed_checkpoint", CATEGORY_TRAX):
        metrics.add_signed_checkpoint_create()
    with metrics.measure("checkpoint.verify_signed_checkpoint", CATEGORY_TRAX):
        metrics.add_signed_checkpoint_verify()
    metrics.add_hash_bound_message()

    counts = metrics.signing_counts_summary()
    assert counts["signed_envelope_create_count"] == 1
    assert counts["signed_envelope_verify_count"] == 1
    assert counts["signed_checkpoint_create_count"] == 1
    assert counts["signed_checkpoint_verify_count"] == 1
    assert counts["hash_bound_message_count"] == 1


def test_compact_json_includes_signing_count_summary():
    metrics = RunMetrics("udp", mode="checkpoint")
    metrics.add_signed_checkpoint_create()
    metrics.add_hash_bound_message()
    compact = metrics.compact_dict()

    assert compact["mode"] == "checkpoint"
    assert compact["signing_counts"]["signed_checkpoint_create_count"] == 1
    assert compact["counts"]["hash_bound_message_count"] == 1
    assert "signed_checkpoint_create_event_ms" in compact["micro_highlights"]
