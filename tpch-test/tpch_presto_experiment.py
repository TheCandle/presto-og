#!/usr/bin/env python3
"""
Presto TPCH 多并行度实验脚本（最终修复版）
完全基于 Presto REST API 的 nextUri 链，获取查询执行时间。
"""

import argparse
import csv
import json
import os
import sys
import time
from typing import List, Dict, Optional, Tuple

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
    parser = argparse.ArgumentParser(description="Presto TPCH 多并行度测试脚本（最终版）")
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
    提交查询，持续跟随 nextUri 直到查询结束，返回最终响应 JSON（包含 queryStats）。
    失败或超时返回 None。
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

    # 初始请求
    url = presto_url + "/v1/statement"
    method = "POST"
    data = sql

    start_time = time.time()
    follow_count = 0
    max_follows = 100  # 防止无限循环（复杂查询可能有大量分页）

    while True:
        # 检查超时
        if time.time() - start_time > timeout:
            print(f"DEBUG: 查询执行超时 ({timeout}秒)", file=sys.stderr)
            return None

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
            return None

        # 检查是否有错误
        if "error" in response_json:
            print(f"DEBUG: 查询错误: {response_json['error']}", file=sys.stderr)
            return None

        # 如果 nextUri 存在，继续跟随
        if "nextUri" in response_json:
            url = response_json["nextUri"]
            method = "GET"
            # 不需要 data
            data = None
            continue
        else:
            # 没有 nextUri，表示查询结束，返回最终响应
            print(f"DEBUG: 查询完成，共跟随 {follow_count} 步", file=sys.stderr)
            return response_json

def extract_execution_time(final_response: Dict) -> Optional[float]:
    """
    从最终响应中提取执行时间（毫秒）。
    优先使用 stats.wallTimeMillis（实际执行时间），
    若不存在则回退到 stats.elapsedTimeMillis（总响应时间）。
    """
    try:
        stats = final_response["stats"]
        state = stats.get("state")
        if state != "FINISHED":
            print(f"DEBUG: 查询未成功完成，状态: {state}", file=sys.stderr)
            return None
        
        if "wallTimeMillis" in stats:
            return float(stats["wallTimeMillis"])
        elif "elapsedTimeMillis" in stats:
            print("DEBUG: wallTimeMillis 不存在，使用 elapsedTimeMillis", file=sys.stderr)
            return float(stats["elapsedTimeMillis"])
        else:
            print("DEBUG: 找不到时间字段", file=sys.stderr)
            return None
    except (KeyError, ValueError, TypeError) as e:
        print(f"DEBUG: 提取时间失败: {e}", file=sys.stderr)
        return None

def run_query_once(presto_url: str, catalog: str, schema: str, sql: str,
                   session_params: List[str], timeout: int) -> Optional[float]:
    """
    执行单次查询，返回执行时间（毫秒），失败返回 None。
    """
    final_resp = execute_query_follow_next_uri(presto_url, catalog, schema, sql,
                                                session_params, timeout)
    if not final_resp:
        return None

    # 检查查询是否成功
    if final_resp.get("stats", {}).get("state") != "FINISHED":
        print(f"DEBUG: 查询未成功完成，状态: {final_resp.get('stats', {}).get('state')}", file=sys.stderr)
        return None

    return extract_execution_time(final_resp)

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

    print(f"开始 TPCH 多并行度测试: {args.presto_url}, catalog={args.catalog}, schema={args.schema}")
    print(f"并行度列表: {dop_values}")
    print(f"基础会话参数: {args.session_params}")
    print(f"预热 {args.warmup} 次, 运行 {args.runs} 次")

    for query_id, query_file in query_files:
        with open(query_file, "r") as f:
            sql = f.read().strip()
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
                    # 预热只需要执行，不需要记录时间
                    _ = run_query_once(args.presto_url, args.catalog, args.schema,
                                       sql, session_params, args.timeout)
                    if w % 5 == 0:
                        print(f"    已完成 {w}/{args.warmup} 次预热")

            # 正式运行
            run_times = []
            for r in range(1, args.runs + 1):
                print(f"  运行第 {r} 次...")
                t = run_query_once(args.presto_url, args.catalog, args.schema,
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
