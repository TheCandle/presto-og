#!/usr/bin/env python3
"""
Presto TPCH benchmark script using per-query/per-stage/per-pipeline DOP mapping.

Core idea:
- Read mapping from CSV: query_id,stage_id,pipeline_id,optimal_dop
- For each query, build session parameter:
    native_pipeline_driver_schedule='3:0:12,3:1:6,3:2:10'
- Run TPCH queries with that schedule.

Runtime metric:
- executionTime - totalPlanningTime (milliseconds)
"""

import argparse
import csv
import json
import os
import sys
import time
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple

import requests

# Default values
DEFAULT_PRESTO_URL = "http://localhost:8082"
DEFAULT_CATALOG = "hive"
DEFAULT_SCHEMA = "tpch_test"
DEFAULT_WARMUP = 2
DEFAULT_RUNS = 3
DEFAULT_DOP_LIST = "16"
DEFAULT_SESSION_PARAMS = ["task_concurrency=64"]
DEFAULT_QUERY_DIR = "./tpch-queries/test-query"
DEFAULT_OUTPUT = "presto_results.csv"
DEFAULT_DETAIL = "presto_detail.csv"
DEFAULT_TIMEOUT = 1500
DEFAULT_STATS_DIR = "./tpch-stats"
DEFAULT_STAGE_DETAIL = "stage_detail.csv"
DEFAULT_OPERATOR_DETAIL = "operator_detail.csv"
DEFAULT_PIPELINE_DOP_FILE = os.path.join(os.path.dirname(__file__), "stage_threadblock_optimal_dop.csv")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Presto TPCH benchmark with native_pipeline_driver_schedule"
    )
    parser.add_argument("--presto-url", default=DEFAULT_PRESTO_URL)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--query-dir", default=DEFAULT_QUERY_DIR)
    parser.add_argument("--dop-list", default=DEFAULT_DOP_LIST,
                        help="Global max_drivers_per_task values (comma-separated)")
    parser.add_argument("--session", action="append", dest="session_params", default=[],
                        help="Additional session params. Can be repeated.")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--detail", default=DEFAULT_DETAIL)
    parser.add_argument("--save-stats-dir", default=DEFAULT_STATS_DIR)
    parser.add_argument("--stage-detail", default=DEFAULT_STAGE_DETAIL)
    parser.add_argument("--operator-detail", default=DEFAULT_OPERATOR_DETAIL)
    parser.add_argument("--pipeline-dop-file", default=DEFAULT_PIPELINE_DOP_FILE,
                        help="CSV file mapping query_id/stage_id/pipeline_id -> optimal_dop")
    parser.add_argument("--skip-analyze", action="store_true")
    return parser.parse_args()


def parse_time_str(time_str: str) -> Optional[float]:
    if not time_str:
        return None
    time_str = time_str.strip()
    if time_str.endswith("ms"):
        return float(time_str[:-2])
    if time_str.endswith("s"):
        return float(time_str[:-1]) * 1000
    if time_str.endswith("m"):
        return float(time_str[:-1]) * 60000
    try:
        return float(time_str) * 1000
    except ValueError:
        return None


def sanitize_query_sql(raw_sql: str) -> str:
    """Remove generator-only trailer lines such as `where rownum <= ...`."""
    lines = []
    for line in raw_sql.splitlines():
        if line.strip().lower().startswith("where rownum <="):
            continue
        lines.append(line)
    sql = "\n".join(lines).strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def normalize_dop(value: Any) -> int:
    """Convert CSV optimal_dop value to int safely (e.g. 64.0 -> 64)."""
    return int(round(float(value)))


def load_query_pipeline_dop(pipeline_dop_file: str) -> Dict[int, List[Tuple[int, int, int]]]:
    """
    Load mapping from CSV:
      query_id,thread_block_id,stage_id,pipeline_id,optimal_dop
    Return:
      { query_id: [(stage_id, pipeline_id, optimal_dop), ...] }
    """
    mapping: Dict[int, List[Tuple[int, int, int]]] = {}
    with open(pipeline_dop_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"query_id", "stage_id", "pipeline_id", "optimal_dop"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"pipeline dop file missing required columns: {required}, got {reader.fieldnames}"
            )

        for row in reader:
            if not row:
                continue
            qid = int(row["query_id"])
            sid = int(row["stage_id"])
            pid = int(row["pipeline_id"])
            dop = normalize_dop(row["optimal_dop"])
            mapping.setdefault(qid, []).append((sid, pid, dop))

    # sort each query schedule by (stage_id, pipeline_id)
    for qid in mapping:
        mapping[qid].sort(key=lambda x: (x[0], x[1]))

    return mapping


def build_native_pipeline_driver_schedule(entries: List[Tuple[int, int, int]]) -> str:
    """
    Convert list[(stage_id, pipeline_id, dop)] to:
      "stage:pipeline:dop,stage:pipeline:dop,..."
    """
    return ",".join(f"{sid}:{pid}:{dop}" for sid, pid, dop in entries)


def build_session_headers(base_params: List[str], dop: int,
                          native_pipeline_driver_schedule: Optional[str]) -> List[str]:
    params = [p for p in base_params if not p.startswith("max_drivers_per_task=")]
    params.append(f"max_drivers_per_task={dop}")
    if native_pipeline_driver_schedule:
        encoded_schedule = quote(native_pipeline_driver_schedule, safe="")
        params.append(f"native_pipeline_driver_schedule={encoded_schedule}")
    return params


def execute_query_follow_next_uri(presto_url: str, catalog: str, schema: str, sql: str,
                                  session_params: List[str], timeout: int) -> Tuple[Optional[Dict], Optional[str]]:
    headers = {
        "X-Presto-Catalog": catalog,
        "X-Presto-Schema": schema,
        "X-Presto-User": "tpch_test",
        "X-Presto-Source": "tpch_benchmark",
        "Accept": "application/json",
    }
    if session_params:
        headers["X-Presto-Session"] = ",".join(session_params)

    url = presto_url + "/v1/statement"
    method = "POST"
    data = sql
    query_id = None
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            print(f"DEBUG: Query timeout after {timeout}s", file=sys.stderr)
            return None, None

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

        if query_id is None:
            query_id = response_json.get("id") or response_json.get("queryId")

        if "error" in response_json:
            print(f"DEBUG: Query error: {response_json['error']}", file=sys.stderr)
            return None, None

        if "nextUri" in response_json:
            url = response_json["nextUri"]
            method = "GET"
            data = None
            continue

        return response_json, query_id


def fetch_query_stats(presto_url: str, query_id: str) -> Optional[Dict[str, Any]]:
    if not query_id:
        return None
    url = f"{presto_url.rstrip('/')}/v1/query/{query_id}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 410:
            return None
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def extract_stage_metrics(stage: Dict[str, Any]) -> Dict[str, Any]:
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


def run_analyze(presto_url: str, catalog: str, schema: str, timeout: int = 300) -> bool:
    tables = ["customer", "orders", "lineitem", "supplier", "nation", "region", "part", "partsupp"]
    ok = True
    for table in tables:
        sql = f"ANALYZE {catalog}.{schema}.{table}"
        final_resp, _ = execute_query_follow_next_uri(presto_url, catalog, schema, sql, [], timeout)
        if final_resp is None or final_resp.get("stats", {}).get("state") != "FINISHED":
            ok = False
    return ok


def run_query_once_subtract(presto_url: str, catalog: str, schema: str, sql: str,
                            session_params: List[str], timeout: int,
                            run_number: int, dop: int, query_id_num: int,
                            args) -> Optional[float]:
    final_resp, query_id = execute_query_follow_next_uri(
        presto_url, catalog, schema, sql, session_params, timeout
    )
    if not final_resp:
        return None

    full_stats = fetch_query_stats(presto_url, query_id) if query_id else None

    runtime = None
    if full_stats:
        qstats = full_stats.get("queryStats", {})
        exec_time = parse_time_str(qstats.get("executionTime"))
        plan_time = parse_time_str(qstats.get("totalPlanningTime"))
        if exec_time is not None and plan_time is not None:
            diff = exec_time - plan_time
            if diff >= 0:
                runtime = diff

    if runtime is None:
        return None

    if full_stats:
        if args.save_stats_dir:
            os.makedirs(args.save_stats_dir, exist_ok=True)
            stats_file = os.path.join(
                args.save_stats_dir,
                f"query_{query_id_num}_dop{dop}_run{run_number}.json",
            )
            with open(stats_file, "w", encoding="utf-8") as f:
                json.dump(full_stats, f, indent=2)

        stages = full_stats.get("queryStats", {}).get("stages", [])
        stage_rows = []
        operator_rows = []
        for stage in stages:
            st = extract_stage_metrics(stage)
            st.update({
                "query": query_id_num,
                "dop": dop,
                "run_number": run_number,
                "runtime_ms": runtime,
            })
            stage_rows.append(st)

            for op in stage.get("stageStats", {}).get("operatorSummaries", []):
                ot = extract_operator_metrics(op, stage.get("stageId"))
                ot.update({
                    "query": query_id_num,
                    "dop": dop,
                    "run_number": run_number,
                    "runtime_ms": runtime,
                })
                operator_rows.append(ot)

        if stage_rows:
            file_exists = os.path.isfile(args.stage_detail)
            with open(args.stage_detail, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=stage_rows[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(stage_rows)

        if operator_rows:
            file_exists = os.path.isfile(args.operator_detail)
            with open(args.operator_detail, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=operator_rows[0].keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerows(operator_rows)

    return runtime


def resolve_query_dir(user_query_dir: str) -> Optional[str]:
    """Resolve query directory with fallbacks.

    Priority:
    1) user provided path
    2) common paths near script/repo
    """
    if user_query_dir and os.path.isdir(user_query_dir):
        return user_query_dir

    script_dir = os.path.dirname(__file__)
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))
    candidates = [
        os.path.join(repo_root, "tpch-queries", "test-query"),
        os.path.join(repo_root, "tpch-queries", "test_query"),
        os.path.join(repo_root, "tpch-queries"),
        os.path.join(repo_root, "tpch-queries", "mytpch"),
        os.path.join(repo_root, "tpch-queries", "originaltpch"),
        os.path.join(script_dir, "tpch-queries", "test-query"),
        os.path.join(script_dir, "tpch-queries", "test_query"),
        os.path.join(script_dir, "tpch-queries"),
        os.path.join(script_dir, "tpch-queries", "mytpch"),
        os.path.join(script_dir, "tpch-queries", "originaltpch"),
    ]
    for p in candidates:
        if os.path.isdir(p):
            return p
    return None


def main():
    args = parse_args()

    if not os.path.isfile(args.pipeline_dop_file):
        print(f"Error: pipeline dop file not found: {args.pipeline_dop_file}", file=sys.stderr)
        sys.exit(1)

    query_pipeline_entries = load_query_pipeline_dop(args.pipeline_dop_file)
    schedule_by_query = {
        qid: build_native_pipeline_driver_schedule(entries)
        for qid, entries in query_pipeline_entries.items()
    }
    print(f"Loaded pipeline DOP mapping from {args.pipeline_dop_file} (queries={len(schedule_by_query)})")

    try:
        dop_values = [int(x.strip()) for x in args.dop_list.split(",") if x.strip()]
    except ValueError:
        print(f"Error: Cannot parse dop-list '{args.dop_list}'", file=sys.stderr)
        sys.exit(1)

    resolved_query_dir = resolve_query_dir(args.query_dir)
    if not resolved_query_dir:
        print(f"Error: query dir not found: {args.query_dir}", file=sys.stderr)
        print("Hint: try --query-dir /home/yjh/Project/presto-og/tpch-queries/test-query", file=sys.stderr)
        sys.exit(1)
    if resolved_query_dir != args.query_dir:
        print(f"Query dir not found, fallback to: {resolved_query_dir}")

    query_files = []
    for i in range(1, 23):
    # for i in range(14, 15):
        # support both q1.sql and q1_r1.sql (or q1_r*.sql)
        direct = os.path.join(resolved_query_dir, f"q{i}.sql")
        if os.path.isfile(direct):
            query_files.append((i, direct))
            continue

        matched = None
        prefix = f"q{i}_r"
        for name in sorted(os.listdir(resolved_query_dir)):
            if name.startswith(prefix) and name.endswith(".sql"):
                matched = os.path.join(resolved_query_dir, name)
                break
        if matched:
            query_files.append((i, matched))

    if not query_files:
        print("Error: no TPCH query files found", file=sys.stderr)
        sys.exit(1)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "dop", "runs", "avg_time_ms", "min_time_ms", "max_time_ms", "run_times_ms"])

    with open(args.detail, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "dop", "run_number", "runtime_ms"])

    if not args.skip_analyze:
        print("Running ANALYZE ...")
        _ = run_analyze(args.presto_url, args.catalog, args.schema, args.timeout)

    for query_id, query_file in query_files:
        with open(query_file, "r", encoding="utf-8") as f:
            sql = sanitize_query_sql(f.read())
        if not sql:
            continue

        schedule = schedule_by_query.get(query_id)
        if not schedule:
            print(f"Warning: no pipeline schedule for query {query_id}, skip", file=sys.stderr)
            continue

        for dop in dop_values:
            print(f"Processing q{query_id}, dop={dop}")
            session_params = build_session_headers(args.session_params, dop, schedule)

            if args.warmup > 0:
                for w in range(1, args.warmup + 1):
                    _ = run_query_once_subtract(
                        args.presto_url, args.catalog, args.schema,
                        sql, session_params, args.timeout, w, dop, query_id, args
                    )

            run_times = []
            for r in range(1, args.runs + 1):
                t = run_query_once_subtract(
                    args.presto_url, args.catalog, args.schema,
                    sql, session_params, args.timeout, r, dop, query_id, args
                )
                if t is not None:
                    run_times.append(t)

                with open(args.detail, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([query_id, dop, r, f"{t:.2f}" if t is not None else "FAILED"])

            if run_times:
                avg_time = sum(run_times) / len(run_times)
                min_time = min(run_times)
                max_time = max(run_times)
                run_times_str = ",".join(f"{x:.2f}" for x in run_times)
            else:
                avg_time = min_time = max_time = 0.0
                run_times_str = ""

            with open(args.output, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    query_id,
                    dop,
                    len(run_times),
                    f"{avg_time:.2f}",
                    f"{min_time:.2f}",
                    f"{max_time:.2f}",
                    run_times_str,
                ])

    print("Benchmark completed")
    print(f"Summary: {args.output}")
    print(f"Detail: {args.detail}")


if __name__ == "__main__":
    main()
