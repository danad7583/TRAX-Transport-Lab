from trax_transport_lab.metrics import CATEGORY_TRAX, RunMetrics


def test_compact_and_full_dict_event_shape():
    metrics = RunMetrics("tcp")
    with metrics.measure("trax.hash32", CATEGORY_TRAX):
        pass
    compact = metrics.compact_dict()
    full = metrics.full_dict()
    assert "events" not in compact
    assert "events_by_category" not in compact
    assert "slowest_events" in compact
    assert "events" in full
    assert "events_by_category" in full


def test_slowest_events_respects_limit_and_sort_order():
    metrics = RunMetrics("udp")
    metrics.record_event("middle", CATEGORY_TRAX, 0, 2000)
    metrics.record_event("slow", CATEGORY_TRAX, 0, 3000)
    metrics.record_event("fast", CATEGORY_TRAX, 0, 1000)
    events = metrics.slowest_events(limit=2)
    assert [event["name"] for event in events] == ["slow", "middle"]
