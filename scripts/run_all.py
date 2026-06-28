from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_section(title: str, command: list[str]) -> None:
    print(flush=True)
    print(f"== {title} ==", flush=True)
    print(f"> {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    steps = [
        ("Verify TRAX import", [sys.executable, "-c", "import trax; print(trax)"]),
        ("Run tests", [sys.executable, "-m", "pytest"]),
        ("Run TCP demo: signed-envelope baseline", [sys.executable, "-m", "trax_transport_lab.tcp_demo"]),
        ("Run UDP demo: signed-envelope baseline", [sys.executable, "-m", "trax_transport_lab.udp_demo"]),
        (
            "Run TCP demo: dag-genesis",
            [sys.executable, "-m", "trax_transport_lab.tcp_demo", "--mode", "dag-genesis"],
        ),
        (
            "Run UDP demo: dag-genesis",
            [sys.executable, "-m", "trax_transport_lab.udp_demo", "--mode", "dag-genesis"],
        ),
        (
            "Compare modes: signed-envelope vs dag-genesis UDP",
            [
                sys.executable,
                "scripts/compare_modes.py",
                "--mode-a",
                "signed-envelope",
                "--mode-b",
                "dag-genesis",
                "--transport",
                "udp",
                "--runs",
                "3",
            ],
        ),
        (
            "Compare modes: signed-envelope vs dag-genesis TCP",
            [
                sys.executable,
                "scripts/compare_modes.py",
                "--mode-a",
                "signed-envelope",
                "--mode-b",
                "dag-genesis",
                "--transport",
                "tcp",
                "--runs",
                "3",
            ],
        ),
        (
            "Compare transports: dag-genesis",
            [
                sys.executable,
                "scripts/compare_transports.py",
                "--mode",
                "dag-genesis",
                "--runs",
                "3",
            ],
        ),
    ]

    try:
        for title, command in steps:
            run_section(title, command)
    except subprocess.CalledProcessError as exc:
        return exc.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
