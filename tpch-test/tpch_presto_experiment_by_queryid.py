#!/usr/bin/env python3
"""
Presto TPCH 混合版测试脚本
- 跟随 nextUri 获取最终响应（保证稳定性）
- 同时记录 queryId，尝试从 /v1/query/{queryId} 获取 executionTime
- 若成功则使用 executionTime，否则回退到最终响应的 wallTimeMillis
支持多并行度、预热、多次运行
"""

import argparse
import csv
import json
import os
import sys
import time
from typing import List, Dict, Optional

import requests

DEFAULT_PRESTO_URL = "http://localhost:8082"
DEFAULT_CATALOG = "hive"
DEFAULT_SCHEMA = "tpch_test"
DEFAULT_WARMUP = 2
DEFAULT_RUNS = 3
DEFAULT_DOP_LIST = "1"
DEFAULT_SESSION_PARAMS = []
DEFAULT_QUERY_DIR = "./tpch-queries"
DEFAULT_OUTPUT = "presto_results.csv"
DEFAULT_DETAIL = "presto_detail.csv"
DEFAULT_TIMEOUT = 600

def parse_args():
    parser = argparse.ArgumentParser(description="Presto TPCH 混合版测试脚本（优先 executionTime，回退 wallTime）")
    parser.add_argument("--presto-url", default=DEFAULT_PRESTO_URL, help="Presto Coordinator URL")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Catalog 名称")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema 名称")
    parser.add_argument("--query-dir", default=DEFAULT_QUERY_DIR, help="查询文件目录")
    parser.add_argument("--dop-list", default=DEFAULT_DOP_LIST,
                        help="逗号分隔的 task_concurrency 值列表，例如 '1,2,4,8'")
    parser.add_argument("--session", action="append", dest="session_params", default=[],
                        help="额外的会话参数，例如 'join_distribution_type=BROADCAST'，可多次使用")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP, help="预热次数")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS, help="正式运行次数")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="查询超时时间（秒）")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="摘要输出文件 (CSV)")
    parser.add_argument("--detail", default=DEFAULT_DETAIL, help="详细输出文件 (CSV)")
    return parser.parse_args()

def build_session_headers(base_params: List[str], dop: int) -> List[str]:
    dop_param = f"task_concurrency={dop}"
    filtered = [p for p in base_params if not p.startswith("task_concurrency=")]
    filtered.append(dop_param)
    return filtered

def execute_query_follow_next_uri(presto_url: str, catalog: str, schema: str, sql: str,
                                   session_params: List[str], timeout: int) -> Optional[Dict]:
    """
    提交查询，持续跟随 nextUri 直到查询结束，返回最终响应 JSON。
    同时从初始响应中提取 queryId（如果存在）。
    """
    headers = {
        "X-Presto-Catalog": catalog,
        "X-Presto-Schema": schema,
        "X-Presto-User": "tpch_test",
        "Accept": "application/json"
    }
    if session_params:
        session_str = ",".join(session_params)
        headers["X-Presto-Session"] = session_str

    url = presto_url + "/v1/statement"
    method = "POST"
    data = sql
    query_id = None

    start_time = time.time()
    follow_count = 0
    max_follows = 100

    while True:
        if time.time() - start_time > timeout:
            print(f"DEBUG: 查询执行超时 ({timeout}秒)", file=sys.stderr)
            return None, None

        follow_count += 1
        try:
            if method == "POST":
                resp = requests.post(url, data=data, headers=headers, timeout=30)
            else:
                resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            response_json = resp.json()
        except Exception as e:
            print(f"DEBUG: 请求异常: {e}", file=sys.stderr)
            return None, None

        # 如果是第一次响应，尝试获取 queryId
        if follow_count == 1:
            query_id = response_json.get("id") or response_json.get("queryId")

        if "error" in response_json:
            print(f"DEBUG: 查询错误: {response_json['error']}", file=sys.stderr)
            return None, None

        if "nextUri" in response_json:
            url = response_json["nextUri"]
            method = "GET"
            data = None
            continue
        else:
            print(f"DEBUG: 查询完成，共跟随 {follow_count} 步，queryId={query_id}", file=sys.stderr)
            return response_json, query_id

def get_execution_time_by_query_id(presto_url: str, query_id: str) -> Optional[float]:
    """
    通过 /v1/query/{queryId} 获取 executionTime，返回毫秒数。
    如果查询历史不可用（410）或解析失败，返回 None。
    """
    if not query_id:
        return None
    url = f"{presto_url.rstrip('/')}/v1/query/{query_id}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 410:
            print(f"DEBUG: 查询 {query_id} 的历史信息已不可用 (410)", file=sys.stderr)
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"DEBUG: 获取 queryStats 失败: {e}", file=sys.stderr)
        return None

    try:
        exec_time_str = data["queryStats"]["executionTime"]
    except KeyError:
        print("DEBUG: 响应中无 executionTime", file=sys.stderr)
        return None

    # 解析字符串为毫秒
    exec_time_str = exec_time_str.strip()
    if exec_time_str.endswith("ms"):
        return float(exec_time_str[:-2])
    elif exec_time_str.endswith("s"):
        return float(exec_time_str[:-1]) * 1000
    elif exec_time_str.endswith("m"):
        return float(exec_time_str[:-1]) * 60000
    else:
        try:
            return float(exec_time_str) * 1000
        except ValueError:
            return None

def extract_wall_time(final_response: Dict) -> Optional[float]:
    """
    从最终响应中提取 wallTimeMillis。
    """
    try:
        stats = final_response["stats"]
        if stats.get("state") != "FINISHED":
            print(f"DEBUG: 查询未成功完成，状态: {stats.get('state')}", file=sys.stderr)
            return None
        return float(stats["wallTimeMillis"])
    except (KeyError, ValueError, TypeError) as e:
        print(f"DEBUG: 提取 wallTimeMillis 失败: {e}", file=sys.stderr)
        return None

def run_query_once_hybrid(presto_url: str, catalog: str, schema: str, sql: str,
                          session_params: List[str], timeout: int) -> Optional[float]:
    """
    混合执行：先跟随 nextUri 获取最终响应和 queryId，
    然后尝试通过 queryId 获取 executionTime，失败则回退到 wallTimeMillis。
    """
    final_resp, query_id = execute_query_follow_next_uri(presto_url, catalog, schema, sql,
                                                          session_params, timeout)
    if not final_resp:
        return None

    # 优先尝试 executionTime
    if query_id:
        exec_time = get_execution_time_by_query_id(presto_url, query_id)
        if exec_time is not None:
            return exec_time

    # 回退到 wallTimeMillis
    wall_time = extract_wall_time(final_resp)
    if wall_time is not None:
        print(f"DEBUG: 使用 wallTimeMillis 作为执行时间: {wall_time} ms", file=sys.stderr)
        return wall_time

    return None

def main():
    args = parse_args()

    try:
        dop_values = [int(x.strip()) for x in args.dop_list.split(",")]
    except ValueError:
        print(f"错误: 无法解析 dop-list '{args.dop_list}'", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(args.query_dir):
        print(f"错误: 查询目录 {args.query_dir} 不存在", file=sys.stderr)
        sys.exit(1)

    query_files = []
    for i in range(1, 23):
        fname = f"q{i}.sql"
        fpath = os.path.join(args.query_dir, fname)
        if os.path.isfile(fpath):
            query_files.append((i, fpath))
        else:
            print(f"警告: 未找到查询文件 {fpath}，跳过", file=sys.stderr)

    if not query_files:
        print("错误: 未找到任何查询文件", file=sys.stderr)
        sys.exit(1)

    summary_file = args.output
    detail_file = args.detail

    summary_header = ["query", "dop", "runs", "avg_time_ms", "min_time_ms", "max_time_ms", "run_times_ms"]
    detail_header = ["query", "dop", "run_number", "runtime_ms"]

    with open(summary_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(summary_header)
    with open(detail_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(detail_header)

    print(f"开始 TPCH 混合版测试: {args.presto_url}, catalog={args.catalog}, schema={args.schema}")
    print(f"并行度列表: {dop_values}")
    print(f"基础会话参数: {args.session_params}")
    print(f"预热 {args.warmup} 次, 运行 {args.runs} 次")

    for query_id, query_file in query_files:
        with open(query_file, "r", encoding="utf-8") as f:
            sql = f.read().strip().rstrip(';')
        if not sql:
            print(f"警告: 查询 {query_id} 内容为空，跳过")
            continue

        for dop in dop_values:
            print(f"\n处理查询 {query_id}, dop={dop} ...")
            session_params = build_session_headers(args.session_params, dop)

            # 预热
            if args.warmup > 0:
                print(f"  预热 {args.warmup} 次...")
                for w in range(1, args.warmup + 1):
                    _ = run_query_once_hybrid(args.presto_url, args.catalog, args.schema,
                                              sql, session_params, args.timeout)
                    if w % 5 == 0:
                        print(f"    已完成 {w}/{args.warmup} 次预热")

            # 正式运行
            run_times = []
            for r in range(1, args.runs + 1):
                print(f"  运行第 {r} 次...")
                t = run_query_once_hybrid(args.presto_url, args.catalog, args.schema,
                                          sql, session_params, args.timeout)
                if t is not None:
                    run_times.append(t)
                    print(f"    耗时: {t:.2f} ms")
                else:
                    print(f"    第 {r} 次失败")
                with open(detail_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([query_id, dop, r, f"{t:.2f}" if t else "FAILED"])

            if run_times:
                avg_time = sum(run_times) / len(run_times)
                min_time = min(run_times)
                max_time = max(run_times)
                run_times_str = ",".join(f"{x:.2f}" for x in run_times)
            else:
                avg_time = min_time = max_time = 0
                run_times_str = ""
            with open(summary_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([query_id, dop, len(run_times),
                                 f"{avg_time:.2f}", f"{min_time:.2f}", f"{max_time:.2f}", run_times_str])

            print(f"  查询 {query_id}, dop={dop} 完成，成功次数: {len(run_times)}/{args.runs}, 平均: {avg_time:.2f} ms")

    print("\n实验完成！")
    print(f"摘要文件: {summary_file}")
    print(f"详细文件: {detail_file}")

if __name__ == "__main__":
    main()
