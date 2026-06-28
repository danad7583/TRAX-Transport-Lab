import pytest

from trax_transport_lab.scaled import make_scale_config
from trax_transport_lab.tcp_demo import DAG_GENESIS_MODE, main as tcp_main, run_tcp_demo


@pytest.mark.parametrize("key_mode", ["separate", "shared", "derived"])
def test_key_modes_are_accepted(key_mode):
    result = run_tcp_demo(
        mode=DAG_GENESIS_MODE,
        scale_config=make_scale_config(messages=10, key_mode=key_mode),
    )
    assert result.ok is True
    assert result.metrics.key_mode == key_mode
    assert result.metrics.key_mode_simulated is True


def test_invalid_key_mode_rejected(capsys):
    with pytest.raises(SystemExit):
        tcp_main(["--mode", DAG_GENESIS_MODE, "--messages", "10", "--key-mode", "invalid"])
    captured = capsys.readouterr()
    assert "invalid choice" in captured.err
