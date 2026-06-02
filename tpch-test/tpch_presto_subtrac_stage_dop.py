#!/usr/bin/env python3
"""
Presto TPCH 测试脚本（executionTime - totalPlanningTime）with detailed stage/operator stats.
- Follows nextUri to get final response.
- Records queryId and fetches full queryStats from /v1/query/{queryId}.
- Extracts stage and operator metrics and saves to separate CSV files.
- Optionally saves raw JSON stats for each run.

Usage examples:
  ./tpch_test.py --presto-url http://presto-coord:8080 --query-dir ./tpch-queries --dop-list 1,2,4 --runs 3
  ./tpch_test.py --save-stats-dir ./stats --stage-detail stage.csv --operator-detail operator.csv
"""

import argparse
import csv
import json
import os
import sys
import time
from typing import List, Dict, Optional, Tuple, Any

import requests

# Default values
DEFAULT_PRESTO_URL = "http://localhost:8082"
DEFAULT_CATALOG = "hive"
DEFAULT_SCHEMA = "tpch_test"
DEFAULT_WARMUP = 2
DEFAULT_RUNS = 3
DEFAULT_DOP_LIST = "16"
DEFAULT_SESSION_PARAMS = ["task_concurrency=64"]
DEFAULT_QUERY_DIR = "./tpch-queries"
DEFAULT_OUTPUT = "presto_results.csv"
DEFAULT_DETAIL = "presto_detail.csv"
DEFAULT_TIMEOUT = 1500
DEFAULT_STATS_DIR = "./tpch-stats"          # if set, raw JSON is saved
DEFAULT_STAGE_DETAIL = "stage_detail.csv"
DEFAULT_OPERATOR_DETAIL = "operator_detail.csv"
DEFAULT_STAGE_DOP_FILE = os.path.join(os.path.dirname(__file__), "query_stage_dop")

def run_analyze(presto_url: str, catalog: str, schema: str, timeout: int = 300) -> bool:
    """
    Execute ANALYZE on all TPCH tables (customer, orders, lineitem, supplier, nation, region).
    Returns True if all succeeded, False otherwise.
    """
    tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]
    success = True
    for table in tables:
        sql = f"ANALYZE {catalog}.{schema}.{table}"
        print(f"  Running ANALYZE on {table}...", file=sys.stderr)
        final_resp, query_id = execute_query_follow_next_uri(
            presto_url, catalog, schema, sql, [], timeout
        )
        if final_resp is None or final_resp.get("stats", {}).get("state") != "FINISHED":
            print(f"  ERROR: ANALYZE on {table} failed", file=sys.stderr)
            success = False
        else:
            print(f"  ANALYZE on {table} finished", file=sys.stderr)
    return success

def parse_args():
    parser = argparse.ArgumentParser(description="Presto TPCH 测试脚本 with detailed stage/operator stats")
    parser.add_argument("--presto-url", default=DEFAULT_PRESTO_URL,
                        help="Presto Coordinator URL (default: %(default)s)")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG,
                        help="Catalog name (default: %(default)s)")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA,
                        help="Schema name (default: %(default)s)")
    parser.add_argument("--query-dir", default=DEFAULT_QUERY_DIR,
                        help="Directory containing q1.sql ... q22.sql (default: %(default)s)")
    parser.add_argument("--dop-list", default=DEFAULT_DOP_LIST,
                        help="Comma-separated task_concurrency values (default: %(default)s)")
    parser.add_argument("--session", action="append", dest="session_params", default=[],
                        help="Additional session parameters, e.g. 'join_distribution_type=BROADCAST' (can be repeated)")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                        help="Number of warm-up runs per query/dop (default: %(default)s)")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help="Number of measured runs per query/dop (default: %(default)s)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="Query timeout in seconds (default: %(default)s)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Summary CSV file (default: %(default)s)")
    parser.add_argument("--detail", default=DEFAULT_DETAIL,
                        help="Per‑run CSV file (default: %(default)s)")

    # New arguments for detailed statistics
    parser.add_argument("--save-stats-dir", default=DEFAULT_STATS_DIR,
                        help="If set, save full queryStats JSON to this directory (one file per run)")
    parser.add_argument("--stage-detail", default=DEFAULT_STAGE_DETAIL,
                        help="Stage‑level detail CSV file (default: %(default)s)")
    parser.add_argument("--operator-detail", default=DEFAULT_OPERATOR_DETAIL,
                        help="Operator‑level detail CSV file (default: %(default)s)")
    parser.add_argument("--stage-dop-file", default=DEFAULT_STAGE_DOP_FILE,
                        help="CSV file mapping query_id/stage_id -> dop (default: %(default)s)")

    parser.add_argument("--skip-analyze", action="store_true",
                    help="Skip running ANALYZE before benchmark")

    return parser.parse_args()


def parse_time_str(time_str: str) -> Optional[float]:
    """
    Parse Presto time strings like "1.23s", "456ms", "2.5m" into milliseconds.
    Returns None if parsing fails.
    """
    if not time_str:
        return None
    time_str = time_str.strip()
    if time_str.endswith("ms"):
        return float(time_str[:-2])
    elif time_str.endswith("s"):
        return float(time_str[:-1]) * 1000
    elif time_str.endswith("m"):
        return float(time_str[:-1]) * 60000
    else:
        try:
            return float(time_str) * 1000
        except ValueError:
            return None


def build_session_headers(base_params: List[str], dop: int) -> List[str]:
    """Add task_concurrency to session parameters, replacing any existing."""
    dop_param = f"max_drivers_per_task={dop}"
    # task_param = f"max_tasks_per_stage = 8"
    # partition_buffer_param = f"native_max_page_partitioning_buffer_size = 268435456"
    filtered = [p for p in base_params if not p.startswith("max_drivers_per_task=")]
    filtered.append(dop_param)
    # filtered.append(task_param)
    # filtered.append(partition_buffer_param)
    return filtered


def load_query_stage_dop(stage_dop_file: str) -> Dict[int, Dict[int, int]]:
    """
    Load stage dop mapping from CSV:
      query_id,stage_id,dop
    Returns: {query_id: {stage_id: dop}}
    """
    mapping: Dict[int, Dict[int, int]] = {}
    with open(stage_dop_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            qi = int(row["query_id"])
            si = int(row["stage_id"])
            dop = int(row["dop"])
            mapping.setdefault(qi, {})[si] = dop
    return mapping


def build_native_stage_max_drivers(stage_to_dop: Dict[int, int]) -> str:
    """
    Convert {stage_id: dop} to "0:1,1:8,2:8,..."
    """
    return ",".join(f"{stage_id}:{dop}" for stage_id, dop in sorted(stage_to_dop.items()))


def execute_query_follow_next_uri(presto_url: str, catalog: str, schema: str, sql: str,
                                   session_params: List[str], timeout: int) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Submit a query and follow nextUri until completion.
    Returns (final_response_json, query_id) or (None, None) on error/timeout.
    """
    headers = {
        "X-Presto-Catalog": catalog,
        "X-Presto-Schema": schema,
        "X-Presto-User": "tpch_test",
        "Accept": "application/json"
    }
    if session_params:
        headers["X-Presto-Session"] = ",".join(session_params)

    url = presto_url + "/v1/statement"
    method = "POST"
    data = sql
    query_id = None

    start_time = time.time()
    follow_count = 0

    while True:
        if time.time() - start_time > timeout:
            print(f"DEBUG: Query timeout after {timeout}s", file=sys.stderr)
            return None, None

        follow_count += 1
        try:
            if method == "POST":
                resp = requests.post(url, data=data, headers=headers, timeout=300)
            else:
                resp = requests.get(url, headers=headers, timeout=300)
            resp.raise_for_status()
            response_json = resp.json()
        except Exception as e:
            print(f"DEBUG: Request failed: {e}", file=sys.stderr)
            return None, None

        # Capture query ID from first response
        if follow_count == 1:
            query_id = response_json.get("id") or response_json.get("queryId")

        if "error" in response_json:
            print(f"DEBUG: Query error: {response_json['error']}", file=sys.stderr)
            return None, None

        if "nextUri" in response_json:
            url = response_json["nextUri"]
            method = "GET"
            data = None
            continue
        else:
            print(f"DEBUG: Query finished after {follow_count} follow(s), queryId={query_id}", file=sys.stderr)
            return response_json, query_id


def fetch_query_stats(presto_url: str, query_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch the full query statistics JSON from /v1/query/{queryId}.
    Returns None if unavailable (410) or on error.
    """
    if not query_id:
        return None
    url = f"{presto_url.rstrip('/')}/v1/query/{query_id}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 410:
            print(f"DEBUG: Query {query_id} stats expired (410)", file=sys.stderr)
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"DEBUG: Failed to fetch query stats: {e}", file=sys.stderr)
        return None


def extract_wall_time(final_response: Dict) -> Optional[float]:
    """Extract wallTimeMillis from the final query response."""
    try:
        stats = final_response["stats"]
        if stats.get("state") != "FINISHED":
            print(f"DEBUG: Query not finished, state: {stats.get('state')}", file=sys.stderr)
            return None
        return float(stats["wallTimeMillis"])
    except (KeyError, ValueError, TypeError) as e:
        print(f"DEBUG: Failed to extract wallTimeMillis: {e}", file=sys.stderr)
        return None


def extract_stage_metrics(stage: Dict[str, Any]) -> Dict[str, Any]:
    """Extract relevant metrics from a stage dict."""
    stats = stage.get("stageStats", {})
    return {
        "stageId": stage.get("stageId"),
        "state": stage.get("state"),
        "totalCpuTime_ms": parse_time_str(stats.get("totalCpuTime")),
        "totalScheduledTime_ms": parse_time_str(stats.get("totalScheduledTime")),
        "peakMemory_bytes": stats.get("peakMemory"),
        "physicalInputBytes": stats.get("physicalInputBytes"),
        "physicalInputRows": stats.get("physicalInputRows"),
        "processedInputBytes": stats.get("processedInputBytes"),
        "processedInputRows": stats.get("processedInputRows"),
        "spilledBytes": stats.get("spilledBytes"),
    }


def extract_operator_metrics(operator: Dict[str, Any], stage_id: str) -> Dict[str, Any]:
    """Extract relevant metrics from an operator summary dict."""
    return {
        "stageId": stage_id,
        "operatorId": operator.get("operatorId"),
        "operatorType": operator.get("operatorType"),
        "totalCpuTime_ms": parse_time_str(operator.get("totalCpuTime")),
        "totalScheduledTime_ms": parse_time_str(operator.get("totalScheduledTime")),
        "inputRows": operator.get("inputRows"),
        "inputBytes": operator.get("inputBytes"),
        "outputRows": operator.get("outputRows"),
        "outputBytes": operator.get("outputBytes"),
        "peakMemory_bytes": operator.get("peakMemory"),
    }


def run_query_once_subtract(presto_url: str, catalog: str, schema: str, sql: str,
                            session_params: List[str], timeout: int,
                            run_number: int, dop: int, query_id_num: int,
                            args) -> Optional[float]:
    """
    Execute one query run:
    - Follow nextUri to get final response and queryId.
    - Fetch full queryStats.
    - Compute primary runtime (executionTime - planningTime, fallback to wallTime).
    - Save detailed stage/operator metrics to CSV.
    - Optionally save raw JSON stats.
    Returns the primary runtime in milliseconds, or None on failure.
    """
    final_resp, query_id = execute_query_follow_next_uri(presto_url, catalog, schema, sql,
                                                          session_params, timeout)
    if not final_resp:
        return None

    # Fetch full stats (may be None if expired)
    full_stats = fetch_query_stats(presto_url, query_id) if query_id else None

    # Determine primary runtime
    runtime = None
    if full_stats:
        exec_time = parse_time_str(full_stats.get("queryStats", {}).get("executionTime"))
        plan_time = parse_time_str(full_stats.get("queryStats", {}).get("totalPlanningTime"))
        if exec_time is not None and plan_time is not None:
            diff = exec_time - plan_time
            if diff >= 0:
                runtime = diff
                print(f"DEBUG: Using executionTime - totalPlanningTime = {diff:.2f} ms", file=sys.stderr)
    if runtime is None:
        print(f"NONE runtime", file=sys.stderr)
        runtime = None

    # Save detailed stats if available
    if full_stats:
        # Optionally write raw JSON
        if args.save_stats_dir:
            os.makedirs(args.save_stats_dir, exist_ok=True)
            stats_file = os.path.join(args.save_stats_dir,
                                      f"query_{query_id_num}_dop{dop}_run{run_number}.json")
            with open(stats_file, "w") as f:
                json.dump(full_stats, f, indent=2)

        # Extract stage and operator metrics
        stages = full_stats.get("queryStats", {}).get("stages", [])
        stage_rows = []
        operator_rows = []
        for stage in stages:
            stage_metrics = extract_stage_metrics(stage)
            stage_metrics.update({
                "query": query_id_num,
                "dop": dop,
                "run_number": run_number,
                "runtime_ms": runtime if runtime is not None else -1,
            })
            stage_rows.append(stage_metrics)

            # Operators within this stage
            for op in stage.get("stageStats", {}).get("operatorSummaries", []):
                op_metrics = extract_operator_metrics(op, stage.get("stageId"))
                op_metrics.update({
                    "query": query_id_num,
                    "dop": dop,
                    "run_number": run_number,
                    "runtime_ms": runtime if runtime is not None else -1,
                })
                operator_rows.append(op_metrics)

        # Append stage details
        if stage_rows:
            file_exists = os.path.isfile(args.stage_detail)
            with open(args.stage_detail, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=stage_rows[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(stage_rows)

        # Append operator details
        if operator_rows:
            file_exists = os.path.isfile(args.operator_detail)
            with open(args.operator_detail, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=operator_rows[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(operator_rows)

    return runtime


def main():
    args = parse_args()

    # Load native_stage_max_drivers mapping (optional but expected for your workflow).
    query_stage_dop: Dict[int, Dict[int, int]] = {}
    native_stage_max_drivers_by_query: Dict[int, str] = {}
    if args.stage_dop_file and os.path.isfile(args.stage_dop_file):
        query_stage_dop = load_query_stage_dop(args.stage_dop_file)
        native_stage_max_drivers_by_query = {
            qid: build_native_stage_max_drivers(stage_map)
            for qid, stage_map in query_stage_dop.items()
        }
        print(f"Loaded stage dop mapping from: {args.stage_dop_file} "
              f"(queries={len(native_stage_max_drivers_by_query)})")
    else:
        print(f"Warning: stage dop file not found: {args.stage_dop_file}. "
              f"Will run without native_stage_max_drivers.", file=sys.stderr)

    # Parse dop list
    try:
        dop_values = [int(x.strip()) for x in args.dop_list.split(",")]
    except ValueError:
        print(f"Error: Cannot parse dop-list '{args.dop_list}'", file=sys.stderr)
        sys.exit(1)

    # Locate query files (q1.sql .. q22.sql)
    if not os.path.isdir(args.query_dir):
        print(f"Error: Query directory {args.query_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    query_files = []
    for i in range(1, 23):
        fname = f"q{i}.sql"
        fpath = os.path.join(args.query_dir, fname)
        if os.path.isfile(fpath):
            query_files.append((i, fpath))
        else:
            print(f"Warning: Query file {fpath} not found, skipping", file=sys.stderr)

    if not query_files:
        print("Error: No query files found", file=sys.stderr)
        sys.exit(1)

    # Prepare summary and per‑run CSV files (headers only)
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "dop", "runs", "avg_time_ms", "min_time_ms", "max_time_ms", "run_times_ms"])
    with open(args.detail, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "dop", "run_number", "runtime_ms"])

    print(f"Starting TPCH benchmark (executionTime - totalPlanningTime)")
    print(f"Presto URL: {args.presto_url}, catalog={args.catalog}, schema={args.schema}")
    print(f"Parallelism values: {dop_values}")
    print(f"Base session params: {args.session_params}")
    print(f"Warmup: {args.warmup}, runs: {args.runs}")
    if args.save_stats_dir:
        print(f"Raw JSON stats will be saved to: {args.save_stats_dir}")
    print(f"Stage detail CSV: {args.stage_detail}")
    print(f"Operator detail CSV: {args.operator_detail}")

    # --- 新增：运行 ANALYZE ---
    if not args.skip_analyze:
        print("\n=== Collecting table statistics (ANALYZE) ===")
        if not run_analyze(args.presto_url, args.catalog, args.schema, args.timeout):
            print("Warning: ANALYZE failed for some tables, but continuing...", file=sys.stderr)

    for query_id, query_file in query_files:
        with open(query_file, "r", encoding="utf-8") as f:
            sql = f.read().strip().rstrip(';')
        if not sql:
            print(f"Warning: Query {query_id} is empty, skipping")
            continue

        native_stage_max_drivers = native_stage_max_drivers_by_query.get(query_id)
        if native_stage_max_drivers is None:
            print(f"Warning: No stage dop mapping for query {query_id}, "
                  f"native_stage_max_drivers will not be set for this query.", file=sys.stderr)

        for dop in dop_values:
            print(f"\nProcessing query {query_id}, dop={dop} ...")
            session_params = build_session_headers(args.session_params, dop)
            # if native_stage_max_drivers:
            #     # X-Presto-Session header parsing splits on ',' between properties.
            #     # native_stage_max_drivers value itself is comma-separated, so we percent-encode commas.
            #     # Also: don't wrap the value in quotes; quotes are treated as literal characters by config parsing.
            #     encoded = native_stage_max_drivers.replace(",", "%2C")
            #     session_params.append(f"native_stage_max_drivers={encoded}")

            # Warmup runs
            if args.warmup > 0:
                print(f"  Warming up {args.warmup} times...")
                for w in range(1, args.warmup + 1):
                    _ = run_query_once_subtract(args.presto_url, args.catalog, args.schema,
                                                sql, session_params, args.timeout,
                                                w, dop, query_id, args)   # run_number = w (but not recorded in summary)
                    if w % 5 == 0:
                        print(f"    Completed {w}/{args.warmup} warmups")

            # Measured runs
            run_times = []
            for r in range(1, args.runs + 1):
                print(f"  Run {r}...")
                t = run_query_once_subtract(args.presto_url, args.catalog, args.schema,
                                            sql, session_params, args.timeout,
                                            r, dop, query_id, args)
                if t is not None:
                    run_times.append(t)
                    print(f"    Runtime: {t:.2f} ms")
                else:
                    print(f"    Run {r} FAILED")

                # Write per‑run detail
                with open(args.detail, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([query_id, dop, r, f"{t:.2f}" if t else "FAILED"])

            # Summary for this (query, dop)
            if run_times:
                avg_time = sum(run_times) / len(run_times)
                min_time = min(run_times)
                max_time = max(run_times)
                run_times_str = ",".join(f"{x:.2f}" for x in run_times)
            else:
                avg_time = min_time = max_time = 0.0
                run_times_str = ""
            with open(args.output, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([query_id, dop, len(run_times),
                                 f"{avg_time:.2f}", f"{min_time:.2f}", f"{max_time:.2f}", run_times_str])

            print(f"  Query {query_id}, dop={dop} done. Successful runs: {len(run_times)}/{args.runs}, avg: {avg_time:.2f} ms")

    print("\nBenchmark completed!")
    print(f"Summary:  {args.output}")
    print(f"Per‑run:  {args.detail}")
    print(f"Stage:    {args.stage_detail}")
    print(f"Operator: {args.operator_detail}")
    if args.save_stats_dir:
        print(f"Raw JSON: {args.save_stats_dir}")


if __name__ == "__main__":
    main()
