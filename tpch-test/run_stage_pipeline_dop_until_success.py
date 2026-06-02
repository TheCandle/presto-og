#!/usr/bin/env python3
"""
Run tpch_presto_subtract_stage_pipeline_dop.py in rounds until one FULLY successful round.

A round is considered successful only when:
1) All expected (query, dop, run_number) executions are successful (no FAILED in detail CSV)
2) Summary CSV has no row with runs == 0
3) Child script exits with code 0

On failed round, temporary outputs are deleted and next round starts after sleep.
"""

import argparse
import csv
import os
import shutil
import subprocess
import sys
import time
from typing import List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry stage pipeline benchmark until one fully successful round"
    )

    # wrapper control args
    parser.add_argument("--max-rounds", type=int, default=0,
                        help="0 means infinite retries; otherwise stop after N rounds")
    parser.add_argument("--retry-interval-seconds", type=int, default=30,
                        help="Sleep seconds between failed rounds")
    parser.add_argument("--round-backoff-max-seconds", type=int, default=300,
                        help="Max sleep seconds with linear backoff")

    # all args after `--` are forwarded to child script
    parser.add_argument("child_args", nargs=argparse.REMAINDER,
                        help="Args for tpch_presto_subtract_stage_pipeline_dop.py. Use `--` before child args")

    return parser.parse_args()


def _parse_csv_int(v: str) -> int:
    try:
        return int(float(v))
    except Exception:
        return -1


def validate_round_output(summary_csv: str, detail_csv: str) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if not os.path.isfile(summary_csv):
        reasons.append(f"summary csv not found: {summary_csv}")
    if not os.path.isfile(detail_csv):
        reasons.append(f"detail csv not found: {detail_csv}")
    if reasons:
        return False, reasons

    # Detail must not contain FAILED runtime entries
    failed_rows = 0
    total_detail_rows = 0
    try:
        with open(detail_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_detail_rows += 1
                runtime = (row.get("runtime_ms") or "").strip().upper()
                if runtime == "FAILED" or runtime == "" or runtime == "NONE":
                    failed_rows += 1
    except Exception as e:
        reasons.append(f"failed to read detail csv: {e}")
        return False, reasons

    if total_detail_rows == 0:
        reasons.append("detail csv has no data rows")
    if failed_rows > 0:
        reasons.append(f"detail csv has FAILED/empty rows: {failed_rows}")

    # Summary must have runs > 0 for every row
    zero_run_rows = 0
    total_summary_rows = 0
    try:
        with open(summary_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_summary_rows += 1
                runs = _parse_csv_int((row.get("runs") or "").strip())
                if runs <= 0:
                    zero_run_rows += 1
    except Exception as e:
        reasons.append(f"failed to read summary csv: {e}")
        return False, reasons

    if total_summary_rows == 0:
        reasons.append("summary csv has no data rows")
    if zero_run_rows > 0:
        reasons.append(f"summary csv rows with runs<=0: {zero_run_rows}")

    return len(reasons) == 0, reasons


def strip_leading_double_dash(args: List[str]) -> List[str]:
    if args and args[0] == "--":
        return args[1:]
    return args


def get_child_arg_value(child_args: List[str], key: str, default_value: str) -> str:
    for i, token in enumerate(child_args):
        if token == key and i + 1 < len(child_args):
            return child_args[i + 1]
    return default_value


def main() -> int:
    args = parse_args()
    child_args = strip_leading_double_dash(args.child_args)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    child_script = os.path.join(script_dir, "tpch_presto_subtract_stage_pipeline_dop.py")
    if not os.path.isfile(child_script):
        print(f"Error: child script not found: {child_script}", file=sys.stderr)
        return 2

    # derive output paths (same defaults as child script)
    summary_output = get_child_arg_value(child_args, "--output", "presto_results.csv")
    detail_output = get_child_arg_value(child_args, "--detail", "presto_detail.csv")

    # use temp files per round and commit on success
    temp_summary = summary_output + ".round_tmp"
    temp_detail = detail_output + ".round_tmp"

    base_cmd = [sys.executable, child_script] + child_args

    round_idx = 1
    while True:
        if args.max_rounds > 0 and round_idx > args.max_rounds:
            print(f"Reached max rounds ({args.max_rounds}) without full success", file=sys.stderr)
            return 1

        # ensure per-round clean temp files
        for p in [temp_summary, temp_detail]:
            if os.path.exists(p):
                os.remove(p)

        round_cmd = base_cmd + ["--output", temp_summary, "--detail", temp_detail]

        print(f"\n===== Round {round_idx} start =====")
        print("Command:", " ".join(round_cmd))

        rc = subprocess.run(round_cmd).returncode

        ok, reasons = validate_round_output(temp_summary, temp_detail)

        if rc == 0 and ok:
            # commit success outputs atomically-ish
            shutil.move(temp_summary, summary_output)
            shutil.move(temp_detail, detail_output)
            print(f"Round {round_idx} FULL SUCCESS")
            print(f"Summary: {summary_output}")
            print(f"Detail: {detail_output}")
            return 0

        print(f"Round {round_idx} FAILED (child_rc={rc})", file=sys.stderr)
        for r in reasons:
            print(f"  - {r}", file=sys.stderr)

        # cleanup failed round temp artifacts
        for p in [temp_summary, temp_detail]:
            if os.path.exists(p):
                os.remove(p)

        sleep_seconds = min(args.retry_interval_seconds * round_idx, args.round_backoff_max_seconds)
        print(f"Sleep {sleep_seconds}s before retry...", file=sys.stderr)
        time.sleep(sleep_seconds)
        round_idx += 1


if __name__ == "__main__":
    raise SystemExit(main())
