from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_URL = "https://github.com/danad7583/TRAX"
ROOT = Path(__file__).resolve().parents[1]
EXTERNAL = ROOT / "external"
TRAX_DIR = EXTERNAL / "TRAX"


def run(command: list[str], cwd: Path | None = None) -> None:
    print(f"> {' '.join(command)}")
    subprocess.run(command, cwd=cwd, check=True)


def main() -> int:
    EXTERNAL.mkdir(exist_ok=True)

    try:
        if TRAX_DIR.exists():
            print(f"external/TRAX already exists; leaving local changes untouched: {TRAX_DIR}")
        else:
            run(["git", "clone", REPO_URL, str(TRAX_DIR)])

        run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        run([sys.executable, "-m", "pip", "install", "maturin", "pytest"])
        run([sys.executable, "-m", "maturin", "develop"], cwd=TRAX_DIR)
        run([sys.executable, "-c", "import trax; print(trax)"], cwd=ROOT)
    except subprocess.CalledProcessError as exc:
        print(f"TRAX bootstrap failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode

    print("TRAX bootstrap complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
