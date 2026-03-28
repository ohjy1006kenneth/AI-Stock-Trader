from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve()
for _ in range(5):
    if (ROOT_DIR / ".gitignore").exists():
        break
    ROOT_DIR = ROOT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import importlib
import json
import sys

from runtime.common.common import CONFIG_DIR, DIAGNOSTICS_DATA_DIR, EXECUTION_DATA_DIR, LEDGER_DIR, MARKET_DATA_DIR, ROOT, STRATEGY_DATA_DIR, load_contracts, now_iso, read_json, write_json

REQUIRED_PACKAGES = ["yfinance"]
REQUIRED_PATHS = [
    ROOT / "runtime",
    ROOT / "strategy",
    ROOT / "config",
    ROOT / "data",
    ROOT / "ledger",
    ROOT / "reports",
    ROOT / "backtests",
    ROOT / "research",
    ROOT / "requirements.txt",
    ROOT / "runtime" / "pi" / "data" / "build_universe.py",
    ROOT / "runtime" / "pi" / "data" / "fetch_price_data.py",
    ROOT / "runtime" / "pi" / "data" / "fetch_fundamental_data.py",
    ROOT / "strategy" / "quality_filter.py",
    ROOT / "strategy" / "calculate_alpha_score.py",
    ROOT / "strategy" / "sentry_monitor.py",
    ROOT / "strategy" / "portfolio_strategist.py",
    ROOT / "runtime" / "pi" / "execution" / "mock_portfolio_executor.py",
    ROOT / "runtime" / "pi" / "reporting" / "daily_report.py",
    ROOT / "runtime" / "pi" / "reporting" / "trade_alerts.py",
    LEDGER_DIR / "mock_portfolio.json",
    STRATEGY_DATA_DIR / "strategist_decisions.json",
    EXECUTION_DATA_DIR / "execution_log.json",
    CONFIG_DIR / "automation_target.json",
]
JSON_PATHS = [
    LEDGER_DIR / "mock_portfolio.json",
    STRATEGY_DATA_DIR / "strategist_decisions.json",
    EXECUTION_DATA_DIR / "execution_log.json",
    CONFIG_DIR / "automation_target.json",
]
STATUS_PATH = DIAGNOSTICS_DATA_DIR / "preflight_status.json"
TEXT_PATH = DIAGNOSTICS_DATA_DIR / "preflight_status.txt"


def main() -> None:
    errors: list[str] = []
    warnings: list[str] = []

    python_executable = sys.executable
    project_root = str(ROOT)

    for pkg in REQUIRED_PACKAGES:
        try:
            importlib.import_module(pkg)
        except Exception:
            errors.append(f"missing_python_package:{pkg}")

    for path in REQUIRED_PATHS:
        if not path.exists():
            errors.append(f"missing_required_path:{path}")

    for path in JSON_PATHS:
        if not path.exists():
            continue
        try:
            json.loads(path.read_text())
        except Exception as exc:
            errors.append(f"unreadable_json:{path}:{exc}")

    contracts = load_contracts()
    if not contracts:
        warnings.append("data_contracts_not_loaded")

    target = read_json(CONFIG_DIR / "automation_target.json", {})
    if not target.get("channel") or not target.get("to"):
        errors.append("telegram_delivery_target_not_configured")
    elif target.get("channel") != "telegram":
        warnings.append("delivery_channel_not_telegram")

    status = {
        "generated_at": now_iso(),
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "required_packages": REQUIRED_PACKAGES,
        "required_paths_checked": [str(p) for p in REQUIRED_PATHS],
        "python_executable": python_executable,
        "project_root": project_root,
    }
    write_json(STATUS_PATH, status)

    header = [
        f"[runtime] root={project_root}",
        f"[runtime] python={python_executable}",
    ]

    if status["ok"]:
        text = "\n".join(header + [
            "PREFLIGHT OK",
            "- runtime ready",
            "- required packages installed",
            "- required files present",
            "- JSON files readable",
            "- Telegram delivery target configured",
        ]) + "\n"
        TEXT_PATH.write_text(text)
        print(text.strip())
        return

    lines = header + ["PREFLIGHT FAILED", "", "Errors:"]
    lines.extend([f"- {e}" for e in errors] or ["- none"])
    lines.append("")
    lines.append("Warnings:")
    lines.extend([f"- {w}" for w in warnings] or ["- none"])
    text = "\n".join(lines) + "\n"
    TEXT_PATH.write_text(text)
    print(text.strip())
    sys.exit(1)


if __name__ == "__main__":
    main()
