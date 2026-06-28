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


def dag_genesis_scaled_command(
    transport_module: str,
    messages: int,
    agent_key_rotation_cadence: int,
    dag_key_rotation_cadence: int,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        transport_module,
        "--mode",
        "dag-genesis",
        "--messages",
        str(messages),
        "--dag-signing-cadence",
        "8",
        "--agent-key-rotation-cadence",
        str(agent_key_rotation_cadence),
        "--dag-key-rotation-cadence",
        str(dag_key_rotation_cadence),
        "--key-mode",
        "separate",
        "--max-dag-nodes",
        "100000",
    ]


def main() -> int:
    steps = [
        # Core environment / test validation
        ("Verify TRAX import", [sys.executable, "-c", "import trax; print(trax)"]),
        ("Run tests", [sys.executable, "-m", "pytest"]),

        # Default signed-envelope baseline demos
        ("Run TCP demo: signed-envelope baseline", [sys.executable, "-m", "trax_transport_lab.tcp_demo"]),
        ("Run UDP demo: signed-envelope baseline", [sys.executable, "-m", "trax_transport_lab.udp_demo"]),

        # Default dag-genesis demos
        (
            "Run TCP demo: dag-genesis",
            [sys.executable, "-m", "trax_transport_lab.tcp_demo", "--mode", "dag-genesis"],
        ),
        (
            "Run UDP demo: dag-genesis",
            [sys.executable, "-m", "trax_transport_lab.udp_demo", "--mode", "dag-genesis"],
        ),

        # Scaled DAG-genesis validation: UDP
        (
            "Run UDP scaled dag-genesis: 10 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.udp_demo",
                messages=10,
                agent_key_rotation_cadence=0,
                dag_key_rotation_cadence=0,
            ),
        ),
        (
            "Run UDP scaled dag-genesis: 100 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.udp_demo",
                messages=100,
                agent_key_rotation_cadence=100,
                dag_key_rotation_cadence=0,
            ),
        ),
        (
            "Run UDP scaled dag-genesis: 1000 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.udp_demo",
                messages=1000,
                agent_key_rotation_cadence=100,
                dag_key_rotation_cadence=1000,
            ),
        ),

        # Scaled DAG-genesis validation: TCP
        (
            "Run TCP scaled dag-genesis: 10 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.tcp_demo",
                messages=10,
                agent_key_rotation_cadence=0,
                dag_key_rotation_cadence=0,
            ),
        ),
        (
            "Run TCP scaled dag-genesis: 100 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.tcp_demo",
                messages=100,
                agent_key_rotation_cadence=100,
                dag_key_rotation_cadence=0,
            ),
        ),
        (
            "Run TCP scaled dag-genesis: 1000 messages",
            dag_genesis_scaled_command(
                "trax_transport_lab.tcp_demo",
                messages=1000,
                agent_key_rotation_cadence=100,
                dag_key_rotation_cadence=1000,
            ),
        ),

        # Existing mode comparisons
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

        # Scaled compare mode validation
        (
            "Compare modes scaled: signed-envelope vs dag-genesis UDP 1000 messages",
            [
                sys.executable,
                "scripts/compare_modes.py",
                "--mode-a",
                "signed-envelope",
                "--mode-b",
                "dag-genesis",
                "--transport",
                "udp",
                "--messages",
                "1000",
                "--dag-signing-cadence",
                "8",
                "--agent-key-rotation-cadence",
                "100",
                "--dag-key-rotation-cadence",
                "1000",
                "--key-mode",
                "separate",
                "--max-dag-nodes",
                "100000",
                "--runs",
                "3",
            ],
        ),

        # Scaled runner script validation
        (
            "Scale messages: dag-genesis UDP/TCP 10 100 1000",
            [
                sys.executable,
                "scripts/scale_messages.py",
                "--mode",
                "dag-genesis",
                "--counts",
                "10",
                "100",
                "1000",
                "--runs",
                "3",
                "--dag-signing-cadence",
                "8",
                "--agent-key-rotation-cadence",
                "100",
                "--dag-key-rotation-cadence",
                "1000",
                "--key-mode",
                "separate",
                "--max-dag-nodes",
                "100000",
            ],
        ),

        # Transport comparison
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
        print(flush=True)
        print(f"FAILED: command exited with {exc.returncode}", flush=True)
        return exc.returncode

    print(flush=True)
    print("ALL TRAX TRANSPORT LAB CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())