"""
Orchestrator: run every native TSO extractor in sequence.

Designed to be triggered daily by Windows Task Scheduler after midnight UTC,
when most European TSOs have finalized the previous gas day.

Each extractor is isolated: a failure in one doesn't stop the rest. All stdout
is captured and appended to a timestamped log file in python/logs/. Exit code
reflects whether any script failed (useful for Task Scheduler's "treat as error"
behavior).

Usage:
  python python/gas_native_daily.py

Environment:
  All SUPABASE_*, ENTSOE_API_TOKEN, GAS_NATIVE_*_START_YEAR variables pass through
  to the child scripts.
"""
from __future__ import annotations
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
LOG_DIR = HERE / "logs"
LOG_DIR.mkdir(exist_ok=True)

SCRIPTS = [
    ("FR", "gas_native_fr_grtgaz.py"),
    ("AT", "gas_native_at_aggm.py"),
    ("DK", "gas_native_dk_energinet.py"),
    ("DE", "gas_native_de_the.py"),  # slowest (ENTSO-E), runs last
]


def main() -> int:
    started = datetime.now(timezone.utc)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    log_path = LOG_DIR / f"gas_native_{stamp}.log"

    def log(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    log(f"=== gas_native_daily started  log={log_path}")
    failed = []
    for cc, script in SCRIPTS:
        log(f"--- {cc}: running {script}")
        t0 = time.time()
        script_path = HERE / script
        try:
            r = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(HERE.parent),
                capture_output=True,
                text=True,
                timeout=60 * 45,
                check=False,
            )
            dt = time.time() - t0
            if r.stdout:
                for line in r.stdout.splitlines():
                    log(f"    {cc} | {line}")
            if r.stderr:
                for line in r.stderr.splitlines():
                    log(f"    {cc} ! {line}")
            if r.returncode != 0:
                failed.append(cc)
                log(f"--- {cc}: FAILED (exit={r.returncode}) in {dt:.1f}s")
            else:
                log(f"--- {cc}: ok in {dt:.1f}s")
        except subprocess.TimeoutExpired:
            failed.append(cc)
            log(f"--- {cc}: TIMEOUT after 45 min")
        except Exception as e:
            failed.append(cc)
            log(f"--- {cc}: EXCEPTION {e!r}")

    total_dt = (datetime.now(timezone.utc) - started).total_seconds()
    if failed:
        log(f"=== gas_native_daily finished with failures: {failed} (total {total_dt:.0f}s)")
        return 1
    log(f"=== gas_native_daily finished OK in {total_dt:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
