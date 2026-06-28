from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, SIGNED_ENVELOPE_MODE
from trax_transport_lab.tcp_demo import main as tcp_main


def test_signed_envelope_output_does_not_print_dag_genesis_interpretation(capsys):
    exit_code = tcp_main(["--mode", SIGNED_ENVELOPE_MODE])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Signed-envelope mode is the conservative baseline." in captured.out
    assert "DAG-genesis mode removes AAIP packet signing from the hot path." not in captured.out


def test_dag_genesis_output_prints_dag_genesis_interpretation(capsys):
    exit_code = tcp_main(["--mode", DAG_GENESIS_MODE])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "DAG-genesis mode removes AAIP packet signing from the hot path." in captured.out
