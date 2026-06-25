from trax_transport_lab.dag_model import DagError, DemoDag
from trax_transport_lab.tcp_demo import run_tcp_demo


def assert_rejected(case: str):
    result = run_tcp_demo(adverse_case=case)
    assert result.ok is False
    assert any(line.startswith("rejected ") for line in result.log_lines)
    assert len(result.dag_nodes) < 2
    return result


def test_malformed_init_rejected():
    assert_rejected("malformed_init")


def test_wrong_receiver_rejected():
    result = assert_rejected("wrong_receiver_init")
    assert any("wrong receiver_public_key" in line for line in result.log_lines)


def test_payload_before_ack_rejected():
    result = assert_rejected("payload_before_ack")
    assert any("expected TRAX_REQ" in line or "missing required fields" in line for line in result.log_lines)


def test_payload_hash_mismatch_rejected():
    result = assert_rejected("payload_hash_mismatch")
    assert any("payload hash mismatch" in line for line in result.log_lines)


def test_wrong_session_rejected():
    result = assert_rejected("wrong_session")
    assert any("wrong session_id" in line for line in result.log_lines)


def test_oversized_frame_rejected():
    result = assert_rejected("oversized_frame")
    assert any("exceeds" in line for line in result.log_lines)


def test_truncated_frame_rejected():
    result = assert_rejected("truncated_frame")
    assert any("remaining" in line or "timed out" in line for line in result.log_lines)


def test_dag_wrong_previous_tip_rejected():
    dag = DemoDag()
    session_id = bytes.fromhex("11" * 32)
    packets = {"A": bytes.fromhex("aa" * 32)}
    dag.append_node("SESSION_START_V0", session_id, [], packets)
    try:
        dag.append_node("STREAM_EXCHANGE_V0", session_id, [bytes.fromhex("bb" * 32)], packets)
    except DagError as exc:
        assert "current final tip" in str(exc)
    else:
        raise AssertionError("wrong previous tip should be rejected")
