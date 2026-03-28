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

from runtime.common.common import CONFIG_DIR, DIAGNOSTICS_DATA_DIR, EXECUTION_DATA_DIR, LEDGER_DIR, ROOT, MARKET_DATA_DIR, env_str, load_contracts, load_execution_config, load_local_env_file, now_iso, read_json, write_json

REQUIRED_PACKAGES = ["yfinance"]
REQUIRED_PATHS = [
    ROOT / "pi_edge" / "fetchers" / "build_universe.py",
    ROOT / "pi_edge" / "fetchers" / "refresh_sp500_constituents.py",
    ROOT / "pi_edge" / "fetchers" / "fetch_price_data.py",
    ROOT / "pi_edge" / "fetchers" / "fetch_fundamental_data.py",
    ROOT / "pi_edge" / "execution" / "alpaca_paper.py",
    ROOT / "pi_edge" / "execution" / "paper_portfolio_executor.py",
    ROOT / "pi_edge" / "reporting" / "daily_report.py",
    ROOT / "pi_edge" / "reporting" / "trade_alerts.py",
    ROOT / "pi_edge" / "reporting" / "pipeline_run_summary.py",
    ROOT / "pi_edge" / "run_daily_cron.sh",
    CONFIG_DIR / "execution.json",
    CONFIG_DIR / "sp500_constituents.json",
    CONFIG_DIR / "automation_target.json",
    LEDGER_DIR / "paper_portfolio.json",
]
JSON_PATHS = [
    CONFIG_DIR / "execution.json",
    CONFIG_DIR / "sp500_constituents.json",
    CONFIG_DIR / "automation_target.json",
    LEDGER_DIR / "paper_portfolio.json",
]
STATUS_PATH = DIAGNOSTICS_DATA_DIR / "preflight_status.json"
TEXT_PATH = DIAGNOSTICS_DATA_DIR / "preflight_status.txt"


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

    load_local_env_file(CONFIG_DIR / "alpaca.env")
    execution_cfg = load_execution_config()
    if execution_cfg.get("broker") != "alpaca":
        errors.append("unsupported_broker")
    if not env_str("ALPACA_API_KEY"):
        errors.append("missing_env:ALPACA_API_KEY")
    if not env_str("ALPACA_API_SECRET"):
        errors.append("missing_env:ALPACA_API_SECRET")
    if execution_cfg.get("paper_trading_only") is not True:
        errors.append("paper_trading_only_must_be_true")

    contracts = load_contracts()
    if not contracts:
        warnings.append("data_contracts_not_loaded")

    snapshot = read_json(CONFIG_DIR / "sp500_constituents.json", {})
    if len(snapshot.get("tickers", [])) < 400:
        errors.append("sp500_snapshot_too_small")

    status = {
        "generated_at": now_iso(),
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "python_executable": sys.executable,
        "project_root": str(ROOT),
        "market_snapshot_present": (MARKET_DATA_DIR / "universe.json").exists(),
        "execution_log_present": (EXECUTION_DATA_DIR / "execution_log.json").exists(),
    }
    write_json(STATUS_PATH, status)

    if status["ok"]:
        text = "\n".join([
            f"[runtime] root={ROOT}",
            f"[runtime] python={sys.executable}",
            "PREFLIGHT OK",
            "- pi_edge runtime files present",
            "- Alpaca paper credentials configured",
            "- S&P 500 snapshot present",
        ]) + "\n"
        TEXT_PATH.write_text(text)
        print(text.strip())
        return

    text = "\n".join([
        f"[runtime] root={ROOT}",
        f"[runtime] python={sys.executable}",
        "PREFLIGHT FAILED",
        "",
        "Errors:",
        *[f"- {e}" for e in errors],
        "",
        "Warnings:",
        *([f"- {w}" for w in warnings] if warnings else ["- none"]),
    ]) + "\n"
    TEXT_PATH.write_text(text)
    print(text.strip())
    sys.exit(1)


if __name__ == "__main__":
    main()
