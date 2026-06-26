from trax_transport_lab.tcp_demo import run_tcp_demo
from trax_transport_lab.udp_demo import run_udp_demo


def test_tcp_and_udp_expose_comparable_metric_keys():
    tcp = run_tcp_demo()
    udp = run_udp_demo()

    assert tcp.final_tip is not None
    assert udp.final_tip is not None
    assert tcp.metrics.dag_nodes_appended == 2
    assert udp.metrics.dag_nodes_appended == 2

    tcp_buckets = set(tcp.metrics.bucket_summary())
    udp_buckets = set(udp.metrics.bucket_summary())
    assert tcp_buckets == udp_buckets

    tcp_events = set(tcp.metrics.key_event_summary())
    udp_events = set(udp.metrics.key_event_summary())
    assert tcp_events == udp_events
