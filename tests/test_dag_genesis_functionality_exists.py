from scripts import compare_modes, compare_transports
from trax_transport_lab.metrics import RunMetrics
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, MODE_CHOICES
from trax_transport_lab.tcp_demo import main as tcp_main
from trax_transport_lab.udp_demo import main as udp_main


def test_tcp_demo_accepts_dag_genesis(capsys):
    exit_code = tcp_main(["--mode", DAG_GENESIS_MODE, "--json"])
    capsys.readouterr()
    assert exit_code == 0
    assert DAG_GENESIS_MODE in MODE_CHOICES


def test_udp_demo_accepts_dag_genesis(capsys):
    exit_code = udp_main(["--mode", DAG_GENESIS_MODE, "--json"])
    capsys.readouterr()
    assert exit_code == 0


def test_compare_modes_supports_mode_a_and_mode_b(capsys):
    exit_code = compare_modes.main(
        [
            "--mode-a",
            "signed-envelope",
            "--mode-b",
            DAG_GENESIS_MODE,
            "--transport",
            "udp",
            "--runs",
            "1",
            "--json",
        ]
    )
    capsys.readouterr()
    assert exit_code == 0


def test_compare_transports_supports_dag_genesis(capsys):
    exit_code = compare_transports.main(
        ["--mode", DAG_GENESIS_MODE, "--runs", "1", "--json"]
    )
    capsys.readouterr()
    assert exit_code == 0


def test_metrics_expose_dag_genesis_counts():
    counts = RunMetrics("tcp", mode=DAG_GENESIS_MODE).signing_counts_summary()
    assert "signed_genesis_create_count" in counts
    assert "signed_genesis_verify_count" in counts
    assert "hot_path_signed_packet_count" in counts
    assert "hash_bound_message_count" in counts
