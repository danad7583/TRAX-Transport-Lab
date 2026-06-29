from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "cloud" / "aws" / "cloudformation" / "scripts" / "bootstrap-trax-tcp-lab.sh"
TEMPLATE = ROOT / "cloud" / "aws" / "cloudformation" / "trax-tcp-lab-ec2.yaml"
GITIGNORE = ROOT / ".gitignore"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_bootstrap_uses_python_311():
    text = read(BOOTSTRAP)

    assert 'PYTHON_BIN="python3.11"' in text
    assert '"${PYTHON_BIN}" -m venv "${VENV_DIR}"' in text


def test_bootstrap_installs_and_builds_trax_core():
    text = read(BOOTSTRAP)

    assert "https://github.com/danad7583/TRAX.git" in text
    assert "external/TRAX" in text
    assert "maturin develop" in text
    assert "import trax; print('trax import OK')" in text


def test_bootstrap_runs_pytest_and_smoke_tests():
    text = read(BOOTSTRAP)

    assert "python -m pytest" in text
    assert "python -m trax_transport_lab.tcp_demo --mode dag-genesis --messages 10" in text
    assert "python -m trax_transport_lab.udp_demo --mode dag-genesis --messages 10" in text


def test_pem_files_are_ignored():
    assert "*.pem" in read(GITIGNORE).splitlines()


def test_lab_key_is_not_tracked():
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "trax-tcp-lab-key.pem"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    assert result.returncode != 0


def test_peer_tcp_ingress_is_private_self_reference():
    text = read(TEMPLATE)

    assert "TraxPeerTcpIngress:" in text
    assert "FromPort: 39100" in text
    assert "ToPort: 39100" in text
    assert "SourceSecurityGroupId: !Ref TraxTcpLabSecurityGroup" in text
    peer_rule = text[text.index("TraxPeerTcpIngress:") : text.index("TraxTcpLabInstanceRole:")]
    assert "CidrIp: 0.0.0.0/0" not in peer_rule
