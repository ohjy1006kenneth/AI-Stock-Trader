from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

from common import CONFIG_DIR, LEDGER_DIR, OUTPUTS_DIR, ROOT, load_contracts, now_iso, read_json, write_json

REQUIRED_PACKAGES = ["yfinance"]
REQUIRED_PATHS = [
    ROOT / "scripts",
    ROOT / "config",
    ROOT / "outputs",
    ROOT / "ledger",
    ROOT / "reports",
    ROOT / "backtests",
    ROOT / "research",
    ROOT / "requirements.txt",
    ROOT / "scripts" / "build_universe.py",
    ROOT / "scripts" / "fetch_price_data.py",
    ROOT / "scripts" / "fetch_fundamental_data.py",
    ROOT / "scripts" / "quality_filter.py",
    ROOT / "scripts" / "calculate_alpha_score.py",
    ROOT / "scripts" / "sentry_monitor.py",
    ROOT / "scripts" / "portfolio_strategist.py",
    ROOT / "scripts" / "mock_portfolio_executor.py",
    ROOT / "scripts" / "daily_report.py",
    ROOT / "scripts" / "trade_alerts.py",
    LEDGER_DIR / "mock_portfolio.json",
    OUTPUTS_DIR / "strategist_decisions.json",
    OUTPUTS_DIR / "execution_log.json",
    CONFIG_DIR / "automation_target.json",
]
JSON_PATHS = [
    LEDGER_DIR / "mock_portfolio.json",
    OUTPUTS_DIR / "strategist_decisions.json",
    OUTPUTS_DIR / "execution_log.json",
    CONFIG_DIR / "automation_target.json",
]
STATUS_PATH = OUTPUTS_DIR / "preflight_status.json"
TEXT_PATH = OUTPUTS_DIR / "preflight_status.txt"


def main() -> None:
    errors: list[str] = []
    warnings: list[str] = []

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
    }
    write_json(STATUS_PATH, status)

    if status["ok"]:
        text = "PREFLIGHT OK\n- runtime ready\n- required packages installed\n- required files present\n- JSON files readable\n- Telegram delivery target configured\n"
        TEXT_PATH.write_text(text)
        print(text.strip())
        return

    lines = ["PREFLIGHT FAILED", "", "Errors:"]
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
