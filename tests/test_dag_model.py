import pytest

from trax_transport_lab.dag_model import DagError, DemoDag


SESSION_ID = bytes.fromhex("11" * 32)
PACKETS = {"A": bytes.fromhex("aa" * 32), "B": bytes.fromhex("bb" * 32)}


def test_session_start_node_hash_deterministic():
    dag_a = DemoDag()
    dag_b = DemoDag()
    node_a = dag_a.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    node_b = dag_b.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    assert node_a.node_hash == node_b.node_hash
    assert node_a.content_hash == node_b.content_hash


def test_stream_exchange_node_hash_deterministic():
    dag_a = DemoDag()
    dag_b = DemoDag()
    first_a = dag_a.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    first_b = dag_b.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    stream_a = dag_a.append_node("STREAM_EXCHANGE_V0", SESSION_ID, [first_a.node_hash], PACKETS)
    stream_b = dag_b.append_node("STREAM_EXCHANGE_V0", SESSION_ID, [first_b.node_hash], PACKETS)
    assert stream_a.node_hash == stream_b.node_hash
    assert stream_a.content_hash == stream_b.content_hash


def test_parent_tip_advances():
    dag = DemoDag()
    first = dag.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    assert dag.final_tip() == first.node_hash
    second = dag.append_node("STREAM_EXCHANGE_V0", SESSION_ID, [first.node_hash], PACKETS)
    assert dag.final_tip() == second.node_hash


def test_wrong_previous_tip_rejected():
    dag = DemoDag()
    dag.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    with pytest.raises(DagError):
        dag.append_node("STREAM_EXCHANGE_V0", SESSION_ID, [bytes.fromhex("cc" * 32)], PACKETS)


def test_enumeration_order():
    dag = DemoDag()
    first = dag.append_node("SESSION_START_V0", SESSION_ID, [], PACKETS)
    second = dag.append_node("STREAM_EXCHANGE_V0", SESSION_ID, [first.node_hash], PACKETS)
    assert dag.enumerate() == [first, second]
