from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPTS = [
    "macro_scout.py",
    "technical_analyst.py",
    "portfolio_risk_manager.py",
    "mock_execution_agent.py",
    "reporting.py",
]

ROOT = Path(__file__).resolve().parent


def main() -> None:
    for script in SCRIPTS:
        print(f"Running {script}...")
        subprocess.run(["python3", str(ROOT / script)], check=True)


if __name__ == "__main__":
    main()
