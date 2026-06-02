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
import math
import os
import re
import subprocess
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
DEFAULT_DBGEN_DIR = "./tpch-dbgen"
DEFAULT_GENERATED_QUERY_COUNT = 0
DEFAULT_SCALE_FACTOR = 100
DEFAULT_OUTPUT = "presto_results.csv"
DEFAULT_DETAIL = "presto_detail.csv"
DEFAULT_TIMEOUT = 1500
DEFAULT_STATS_DIR = "./tpch-stats"          # if set, raw JSON is saved
DEFAULT_STAGE_DETAIL = "stage_detail.csv"
DEFAULT_OPERATOR_DETAIL = "operator_detail.csv"


def split_sql_statements(sql: str) -> List[str]:
    """Split SQL text by ';' and return non-empty statements."""
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


def ensure_explain_analyze_prefix(sql: str) -> str:
    """Ensure the SQL starts with EXPLAIN ANALYZE."""
    if re.match(r"^\s*EXPLAIN\s+ANALYZE\b", sql, flags=re.IGNORECASE):
        return sql.strip()
    return f"EXPLAIN ANALYZE\n{sql.strip()}"


def prepare_query_statements(query_id_num: int, sql: str) -> Tuple[List[str], str, List[str]]:
    """
    Return (setup_statements, measured_statement, cleanup_statements).
    - For normal queries, measured statement is the only SQL prefixed with EXPLAIN ANALYZE.
    - For q15 with create/select/drop style SQL, run create as setup, explain-analyze the select,
      and run drop as cleanup.
    """
    statements = split_sql_statements(sql)
    if not statements:
        return [], "", []

    if query_id_num == 15 and len(statements) >= 3:
        first_stmt = statements[0]
        last_stmt = statements[-1]
        has_create = re.match(r"^\s*create\s+view\b", first_stmt, flags=re.IGNORECASE)
        has_drop = re.match(r"^\s*drop\s+view\b", last_stmt, flags=re.IGNORECASE)
        if has_create and has_drop:
            select_stmt = ""
            for stmt in statements[1:-1]:
                if re.match(r"^\s*select\b", stmt, flags=re.IGNORECASE):
                    select_stmt = stmt
                    break
            if select_stmt:
                return [first_stmt], ensure_explain_analyze_prefix(select_stmt), [last_stmt]

    measured_sql = ensure_explain_analyze_prefix(statements[0])
    return [], measured_sql, []

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
    parser.add_argument("--generate-query-count", type=int, default=DEFAULT_GENERATED_QUERY_COUNT,
                        help="If > 0, generate this many TPCH SQL files via qgen before running")
    parser.add_argument("--dbgen-dir", default=DEFAULT_DBGEN_DIR,
                        help="tpch-dbgen directory containing qgen binary and queries template")
    parser.add_argument("--scale-factor", type=int, default=DEFAULT_SCALE_FACTOR,
                        help="Scale factor passed to qgen -s (default: %(default)s)")
    parser.add_argument("--overwrite-generated", action="store_true",
                        help="When generating SQLs, remove old generated q*_r*.sql files first")
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


def parse_query_file_info(path: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract (query_num, round_num) from file names like q1.sql, q15_r3.sql."""
    name = os.path.basename(path)
    match = re.match(r"^q(\d+)(?:_r(\d+))?\.sql$", name, flags=re.IGNORECASE)
    if not match:
        return None, None
    query_num = int(match.group(1))
    round_num = int(match.group(2)) if match.group(2) else 1
    return query_num, round_num


def detect_query_id_from_filename(path: str) -> Optional[int]:
    """Extract query id from file names like q1.sql, q15_r3.sql."""
    query_num, _ = parse_query_file_info(path)
    return query_num


def collect_query_files(query_dir: str) -> List[Tuple[int, str]]:
    """
    Collect SQL files in query_dir.
    Priority:
    1) generated files q*_r*.sql (sorted by round/query id)
    2) fallback to base q1..q22.sql
    """
    generated = []
    for fname in os.listdir(query_dir):
        if not re.match(r"^q\d+_r\d+\.sql$", fname, flags=re.IGNORECASE):
            continue
        query_id = detect_query_id_from_filename(fname)
        if query_id is None:
            continue
        round_match = re.search(r"_r(\d+)\.sql$", fname, flags=re.IGNORECASE)
        round_id = int(round_match.group(1)) if round_match else 0
        generated.append((round_id, query_id, os.path.join(query_dir, fname)))

    if generated:
        generated.sort(key=lambda x: (x[0], x[1]))
        return [(query_id, path) for _, query_id, path in generated]

    query_files = []
    for i in range(1, 23):
        fname = f"q{i}.sql"
        fpath = os.path.join(query_dir, fname)
        if os.path.isfile(fpath):
            query_files.append((i, fpath))
        else:
            print(f"Warning: Query file {fpath} not found, skipping", file=sys.stderr)
    return query_files


def generate_tpch_queries(dbgen_dir: str, output_dir: str, total_count: int, scale_factor: int,
                          overwrite_generated: bool) -> List[Tuple[int, str]]:
    """
    Generate total_count SQL files by repeatedly running qgen for q1..q22.
    Output file naming: q{query_id}_r{round}.sql
    """
    if total_count <= 0:
        return []

    dbgen_dir_abs = os.path.abspath(dbgen_dir)
    output_dir_abs = os.path.abspath(output_dir)
    qgen_bin = os.path.join(dbgen_dir_abs, "qgen")
    template_dir = os.path.join(dbgen_dir_abs, "queries")

    if not os.path.isfile(qgen_bin):
        raise RuntimeError(f"qgen binary not found: {qgen_bin}")
    if not os.path.isdir(template_dir):
        raise RuntimeError(f"qgen template directory not found: {template_dir}")

    os.makedirs(output_dir_abs, exist_ok=True)

    if overwrite_generated:
        for fname in os.listdir(output_dir_abs):
            if re.match(r"^q\d+_r\d+\.sql$", fname, flags=re.IGNORECASE):
                os.remove(os.path.join(output_dir_abs, fname))

    env = os.environ.copy()
    env["DSS_QUERY"] = template_dir

    generated_files: List[Tuple[int, str]] = []
    rounds = int(math.ceil(total_count / 22.0))
    print(f"Generating TPCH SQL files: target={total_count}, rounds={rounds}, scale={scale_factor}")

    for round_idx in range(1, rounds + 1):
        for query_id in range(1, 23):
            if len(generated_files) >= total_count:
                break
            out_name = f"q{query_id}_r{round_idx}.sql"
            out_path = os.path.join(output_dir_abs, out_name)

            try:
                result = subprocess.run(
                    ["./qgen", "-s", str(scale_factor), str(query_id)],
                    cwd=dbgen_dir_abs,
                    env=env,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    f"qgen failed on q{query_id}, round {round_idx}: {e.stderr.strip()}"
                ) from e

            sql = result.stdout.strip()
            if not sql:
                raise RuntimeError(f"Generated SQL is empty for q{query_id}, round {round_idx}")

            with open(out_path, "w", encoding="utf-8") as f:
                f.write(sql)
                if not sql.endswith("\n"):
                    f.write("\n")
            generated_files.append((query_id, out_path))

        print(f"  Generated {len(generated_files)}/{total_count}")

    return generated_files


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
                            run_number: int, dop: int, query_id_num: int, query_label: str,
                            args, query_num: Optional[int] = None,
                            round_num: Optional[int] = None) -> Optional[float]:
    """
    Execute one query run:
    - Follow nextUri to get final response and queryId.
    - Fetch full queryStats.
    - Compute primary runtime (executionTime - planningTime, fallback to wallTime).
    - Save detailed stage/operator metrics to CSV.
    - Optionally save raw JSON stats.
    Returns the primary runtime in milliseconds, or None on failure.
    """
    setup_stmts, measured_sql, cleanup_stmts = prepare_query_statements(query_id_num, sql)
    if not measured_sql:
        return None

    for stmt in setup_stmts:
        setup_resp, _ = execute_query_follow_next_uri(presto_url, catalog, schema, stmt,
                                                      session_params, timeout)
        if setup_resp is None or setup_resp.get("stats", {}).get("state") != "FINISHED":
            print(f"DEBUG: Setup statement failed for query {query_id_num}", file=sys.stderr)
            return None

    final_resp, query_id = execute_query_follow_next_uri(presto_url, catalog, schema, measured_sql,
                                                          session_params, timeout)
    if not final_resp:
        for stmt in cleanup_stmts:
            execute_query_follow_next_uri(presto_url, catalog, schema, stmt, session_params, timeout)
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
                                      f"query_{query_label}_dop{dop}_run{run_number}.json")
            with open(stats_file, "w") as f:
                json.dump(full_stats, f, indent=2)

        # Extract stage and operator metrics
        stages = full_stats.get("queryStats", {}).get("stages", [])
        stage_rows = []
        operator_rows = []
        for stage in stages:
            stage_metrics = extract_stage_metrics(stage)
            stage_metrics.update({
                "query": query_label,
                "query_num": query_num if query_num is not None else query_id_num,
                "round_num": round_num if round_num is not None else 1,
                "dop": dop,
                "run_number": run_number,
                "runtime_ms": runtime if runtime is not None else -1,
            })
            stage_rows.append(stage_metrics)

            # Operators within this stage
            for op in stage.get("stageStats", {}).get("operatorSummaries", []):
                op_metrics = extract_operator_metrics(op, stage.get("stageId"))
                op_metrics.update({
                    "query": query_label,
                    "query_num": query_num if query_num is not None else query_id_num,
                    "round_num": round_num if round_num is not None else 1,
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

    for stmt in cleanup_stmts:
        cleanup_resp, _ = execute_query_follow_next_uri(presto_url, catalog, schema, stmt,
                                                        session_params, timeout)
        if cleanup_resp is None or cleanup_resp.get("stats", {}).get("state") != "FINISHED":
            print(f"DEBUG: Cleanup statement failed for query {query_id_num}", file=sys.stderr)

    return runtime


def main():
    args = parse_args()

    # Parse dop list
    try:
        dop_values = [int(x.strip()) for x in args.dop_list.split(",")]
    except ValueError:
        print(f"Error: Cannot parse dop-list '{args.dop_list}'", file=sys.stderr)
        sys.exit(1)

    # Locate/generate query files
    if not os.path.isdir(args.query_dir):
        print(f"Error: Query directory {args.query_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    if args.generate_query_count < 0:
        print("Error: --generate-query-count must be >= 0", file=sys.stderr)
        sys.exit(1)

    if args.generate_query_count > 0:
        try:
            query_files = generate_tpch_queries(
                dbgen_dir=args.dbgen_dir,
                output_dir=args.query_dir,
                total_count=args.generate_query_count,
                scale_factor=args.scale_factor,
                overwrite_generated=args.overwrite_generated,
            )
        except Exception as e:
            print(f"Error: failed to generate TPCH SQLs: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        query_files = collect_query_files(args.query_dir)

    if not query_files:
        print("Error: No query files found", file=sys.stderr)
        sys.exit(1)

    # Prepare summary and per‑run CSV files (headers only)
    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "query_num", "round_num", "dop", "runs", "avg_time_ms", "min_time_ms", "max_time_ms", "run_times_ms"])
    with open(args.detail, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "query_num", "round_num", "dop", "run_number", "runtime_ms"])

    print(f"Starting TPCH benchmark (executionTime - totalPlanningTime)")
    print(f"Presto URL: {args.presto_url}, catalog={args.catalog}, schema={args.schema}")
    print(f"Parallelism values: {dop_values}")
    print(f"Base session params: {args.session_params}")
    print(f"Warmup: {args.warmup}, runs: {args.runs}")
    print(f"Total SQL files to run: {len(query_files)}")
    if args.generate_query_count > 0:
        print(f"Generated SQL count: {args.generate_query_count} (from {args.dbgen_dir})")
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
        query_label = os.path.splitext(os.path.basename(query_file))[0]
        query_num, round_num = parse_query_file_info(query_file)
        with open(query_file, "r", encoding="utf-8") as f:
            sql = f.read().strip()
        if not sql:
            print(f"Warning: Query {query_label} is empty, skipping")
            continue

        for dop in dop_values:
            print(f"\nProcessing query {query_label} (query_num={query_num}, round_num={round_num}), dop={dop} ...")
            session_params = build_session_headers(args.session_params, dop)

            # Warmup runs
            if args.warmup > 0:
                print(f"  Warming up {args.warmup} times...")
                for w in range(1, args.warmup + 1):
                    _ = run_query_once_subtract(args.presto_url, args.catalog, args.schema,
                                                sql, session_params, args.timeout,
                                                w, dop, query_id, query_label, args,
                                                query_num=query_num, round_num=round_num)   # run_number = w (but not recorded in summary)
                    if w % 5 == 0:
                        print(f"    Completed {w}/{args.warmup} warmups")

            # Measured runs
            run_times = []
            for r in range(1, args.runs + 1):
                print(f"  Run {r}...")
                t = run_query_once_subtract(args.presto_url, args.catalog, args.schema,
                                            sql, session_params, args.timeout,
                                            r, dop, query_id, query_label, args,
                                            query_num=query_num, round_num=round_num)
                if t is not None:
                    run_times.append(t)
                    print(f"    Runtime: {t:.2f} ms")
                else:
                    print(f"    Run {r} FAILED")

                # Write per‑run detail
                with open(args.detail, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        query_label,
                        query_num if query_num is not None else query_id,
                        round_num if round_num is not None else 1,
                        dop,
                        r,
                        f"{t:.2f}" if t is not None else "FAILED"
                    ])

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
                writer.writerow([
                    query_label,
                    query_num if query_num is not None else query_id,
                    round_num if round_num is not None else 1,
                    dop,
                    len(run_times),
                    f"{avg_time:.2f}",
                    f"{min_time:.2f}",
                    f"{max_time:.2f}",
                    run_times_str
                ])

            print(f"  Query {query_label}, dop={dop} done. Successful runs: {len(run_times)}/{args.runs}, avg: {avg_time:.2f} ms")

    print("\nBenchmark completed!")
    print(f"Summary:  {args.output}")
    print(f"Per‑run:  {args.detail}")
    print(f"Stage:    {args.stage_detail}")
    print(f"Operator: {args.operator_detail}")
    if args.save_stats_dir:
        print(f"Raw JSON: {args.save_stats_dir}")


if __name__ == "__main__":
    main()
