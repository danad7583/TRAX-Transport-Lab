import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUN_ALL = ROOT / "scripts" / "run_all.py"


def test_run_all_references_dag_genesis():
    source = RUN_ALL.read_text(encoding="utf-8")
    assert "--mode" in source
    assert "dag-genesis" in source


def test_run_all_references_compare_scripts():
    source = RUN_ALL.read_text(encoding="utf-8")
    assert "compare_modes.py" in source
    assert "compare_transports.py" in source


def test_run_all_keeps_signed_envelope_default_demos():
    source = RUN_ALL.read_text(encoding="utf-8")
    tree = ast.parse(source)
    constants = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant)]
    assert '"Run TCP demo: signed-envelope baseline"' in source
    assert '"Run UDP demo: signed-envelope baseline"' in source
    assert "trax_transport_lab.tcp_demo" in constants
    assert "trax_transport_lab.udp_demo" in constants
