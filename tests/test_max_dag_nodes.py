from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, run_tcp_demo


def test_max_dag_nodes_accepts_100000():
    config = make_scale_config(messages=10, max_dag_nodes=100000)
    assert config.max_dag_nodes == 100000


def test_max_dag_nodes_accepts_1000000_parser_level():
    config = make_scale_config(messages=10, max_dag_nodes=1000000)
    assert config.max_dag_nodes == 1000000


def test_max_dag_nodes_prunes_safely_and_keeps_tip():
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=100, max_dag_nodes=50),
    )
    assert result.ok is True
    assert result.final_tip is not None
    assert result.metrics.dag_nodes_retained == 50
    assert result.metrics.dag_nodes_pruned > 0
