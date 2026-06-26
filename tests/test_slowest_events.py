from trax_transport_lab.metrics import CATEGORY_DAG, CATEGORY_TRAX, RunMetrics


def test_slowest_events_include_expected_fields():
    metrics = RunMetrics("tcp")
    metrics.record_event("a", CATEGORY_DAG, 0, 1000)
    event = metrics.slowest_events()[0]
    assert event["name"] == "a"
    assert event["category"] == CATEGORY_DAG
    assert "duration_ms" in event
    assert "duration_us" in event


def test_slowest_events_sorted_descending():
    metrics = RunMetrics("tcp")
    metrics.record_event("fast", CATEGORY_DAG, 0, 1000)
    metrics.record_event("slow", CATEGORY_TRAX, 0, 2000)
    events = metrics.slowest_events()
    assert [event["name"] for event in events] == ["slow", "fast"]
