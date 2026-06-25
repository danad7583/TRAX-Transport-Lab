from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_section(title: str, command: list[str]) -> None:
    print()
    print(f"== {title} ==")
    print(f"> {' '.join(command)}")
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    steps = [
        ("Verify TRAX import", [sys.executable, "-c", "import trax; print(trax)"]),
        ("Run tests", [sys.executable, "-m", "pytest"]),
        ("Run TCP demo", [sys.executable, "-m", "trax_transport_lab.tcp_demo"]),
        ("Run UDP demo", [sys.executable, "-m", "trax_transport_lab.udp_demo"]),
    ]

    try:
        for title, command in steps:
            run_section(title, command)
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
