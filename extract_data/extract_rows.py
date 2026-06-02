#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import csv
import sys
import subprocess
import json
from urllib import request, error
from datetime import datetime, timezone


def parse_duration_to_ms(value_with_unit):
    """Parse Presto duration string (e.g. 12.3ms/4s/100us/9ns/2m/1h) to milliseconds."""
    if value_with_unit is None:
        return None
    if isinstance(value_with_unit, (int, float)):
        return float(value_with_unit)
    text = str(value_with_unit).strip()
    m = re.match(r'^([\d.,]+)\s*(ns|us|ms|s|m|h)$', text, re.IGNORECASE)
    if not m:
        return None
    value = float(m.group(1).replace(',', ''))
    unit = m.group(2).lower()
    if unit == 'ns':
        return value / 1_000_000.0
    if unit == 'us':
        return value / 1_000.0
    if unit == 'ms':
        return value
    if unit == 's':
        return value * 1000.0
    if unit == 'm':
        return value * 60.0 * 1000.0
    if unit == 'h':
        return value * 3600.0 * 1000.0
    return None


class OperatorNode:
    def __init__(self, name, indent_level, left_input, right_input, output, exec_time):
        self.name = name
        self.indent_level = indent_level
        self.explicit_left = left_input
        self.explicit_right = right_input
        self.explicit_output = output
        self.cpu_time_ms = exec_time
        self.plan_id = None
        self.children = []
        self.parent = None
        self.inferred_left = None
        self.inferred_right = None
        self.inferred_output = None
        self.fragment_id = None
        self.remote_target_fragment = None


def parse_operator_rows(explain_output, debug=False):
    lines = explain_output.splitlines()
    op_pattern = re.compile(r'^\s*-\s*(\w+)')
    remote_source_pattern = re.compile(r'RemoteSource\[(\d+)\]')
    left_pattern = re.compile(r'Left input:\s*([\d,]+)\s*rows?', re.IGNORECASE)
    right_pattern = re.compile(r'Right input:\s*([\d,]+)\s*rows?', re.IGNORECASE)
    input_pattern = re.compile(r'Input:\s*([\d,]+)\s*rows?', re.IGNORECASE)
    output_pattern = re.compile(r'Output:\s*([\d,]+)\s*rows?', re.IGNORECASE)
    raw_input_pattern = re.compile(r'Raw input:\s*([\d,]+)\s*rows?', re.IGNORECASE)
    cpu_pattern = re.compile(r'CPU:\s*([\d.,]+)\s*(ns|us|ms|s|m|h)', re.IGNORECASE)

    # 分割 Fragment
    fragment_pattern = re.compile(r'Fragment (\d+) \[.*\]')
    fragments = []
    current_frag = []
    current_frag_id = None
    for line in lines:
        m = fragment_pattern.match(line)
        if m:
            if current_frag:
                fragments.append((current_frag_id, current_frag))
                current_frag = []
            current_frag_id = int(m.group(1))
            current_frag.append(line)
        else:
            current_frag.append(line)
    if current_frag:
        fragments.append((current_frag_id, current_frag))

    all_nodes = []
    fragment_root_map = {}
    next_plan_id = 0

    for frag_id, frag_lines in fragments:
        # 提取 Fragment 头部的 Output 和 CPU
        frag_output = None
        frag_cpu = None
        for j in range(1, min(6, len(frag_lines))):
            m = output_pattern.search(frag_lines[j])
            if m:
                frag_output = int(m.group(1).replace(',', ''))
            c = cpu_pattern.search(frag_lines[j])
            if c:
                value = float(c.group(1).replace(',', ''))
                unit = c.group(2).lower()
                frag_cpu = parse_duration_to_ms(f"{value}{unit}")
        if debug:
            print(f"Fragment {frag_id} output = {frag_output}, cpu = {frag_cpu}")

        # 提取算子信息
        ops_in_frag = []
        for idx, line in enumerate(frag_lines):
            m = op_pattern.search(line)
            if m:
                indent = len(line) - len(line.lstrip())
                name = m.group(1)
                remote_target = None
                if name == 'RemoteSource':
                    rm = remote_source_pattern.search(line)
                    if rm:
                        remote_target = int(rm.group(1))
                ops_in_frag.append((idx, indent, name, remote_target))

        if not ops_in_frag:
            continue

        nodes = []
        for i, (start_idx, indent, name, remote_target) in enumerate(ops_in_frag):
            end_idx = len(frag_lines)
            if i + 1 < len(ops_in_frag):
                end_idx = ops_in_frag[i+1][0]
            block_lines = frag_lines[start_idx:end_idx]
            block_text = '\n'.join(block_lines)

            left_input = None
            right_input = None
            output = None
            exec_time = None

            left_match = left_pattern.search(block_text)
            if left_match:
                left_input = int(left_match.group(1).replace(',', ''))
            right_match = right_pattern.search(block_text)
            if right_match:
                right_input = int(right_match.group(1).replace(',', ''))

            if left_input is None and right_input is None:
                inp_match = input_pattern.search(block_text)
                if inp_match:
                    left_input = int(inp_match.group(1).replace(',', ''))
                else:
                    raw_match = raw_input_pattern.search(block_text)
                    if raw_match:
                        left_input = int(raw_match.group(1).replace(',', ''))

            out_match = output_pattern.search(block_text)
            if out_match:
                output = int(out_match.group(1).replace(',', ''))

            cpu_match = cpu_pattern.search(block_text)
            if cpu_match:
                value = float(cpu_match.group(1).replace(',', ''))
                unit = cpu_match.group(2).lower()
                if unit == 'ns':
                    exec_time = value / 1_000_000.0      # 纳秒 → 毫秒
                elif unit == 'us':
                    exec_time = value / 1_000.0          # 微秒 → 毫秒
                elif unit == 'ms':
                    exec_time = value
                elif unit == 's':
                    exec_time = value * 1000.0
                elif unit == 'm':
                    exec_time = value * 60.0 * 1000.0
                elif unit == 'h':
                    exec_time = value * 3600.0 * 1000.0
                else:
                    exec_time = None

            node = OperatorNode(name, indent, left_input, right_input, output, exec_time)
            node.fragment_id = frag_id
            node.remote_target_fragment = remote_target
            nodes.append(node)

        # 构建树（同 Fragment）
        stack = []
        for node in nodes:
            while stack and stack[-1][0] >= node.indent_level:
                stack.pop()
            if stack:
                parent = stack[-1][1]
                parent.children.append(node)
                node.parent = parent
            stack.append((node.indent_level, node))

        # 分配 plan_id
        for node in nodes:
            node.plan_id = next_plan_id
            next_plan_id += 1
            if debug:
                print(f"  ID {node.plan_id} -> {node.name} (frag {frag_id})")

        # 确定根节点
        root_nodes = [node for node in nodes if node.parent is None]
        if root_nodes:
            root = root_nodes[0]
            fragment_root_map[frag_id] = root
            if root.explicit_output is None and frag_output is not None:
                root.explicit_output = frag_output
                if debug:
                    print(f"  Assigned frag_output {frag_output} to root {root.name}")

            # === 根节点 CPU 时间计算：Fragment 总时间 - 其余节点 CPU 时间之和 ===
            if frag_cpu is not None:
                other_cpu_sum = 0.0
                for node in nodes:
                    if node is not root and node.cpu_time_ms is not None:
                        other_cpu_sum += node.cpu_time_ms
                root_cpu = frag_cpu - other_cpu_sum
                if root_cpu < 0:
                    root_cpu = 0.0
                root.cpu_time_ms = root_cpu
                if debug:
                    print(f"  Calculated root {root.name} cpu = {root.cpu_time_ms:.2f} (frag_cpu {frag_cpu:.2f} - other_sum {other_cpu_sum:.2f})")
            # 如果 frag_cpu 为 None，则保持根节点原有的 cpu_time_ms（可能为 None）

        all_nodes.extend(nodes)

    # 后序遍历计算输出和输入
    def propagate(node):
        for child in node.children:
            propagate(child)
        # 输出
        if node.explicit_output is not None:
            node.inferred_output = node.explicit_output
        else:
            if node.children:
                node.inferred_output = sum(c.inferred_output for c in node.children if c.inferred_output is not None)
            else:
                node.inferred_output = None
        # 输入
        if node.name == 'RemoteSource':
            node.inferred_left = node.inferred_output if node.inferred_output is not None else 0
            node.inferred_right = None
        else:
            if node.explicit_left is None:
                if len(node.children) == 1:
                    node.inferred_left = node.children[0].inferred_output
                    node.inferred_right = None
                elif len(node.children) >= 2:
                    node.inferred_left = node.children[0].inferred_output
                    node.inferred_right = node.children[1].inferred_output
                else:
                    node.inferred_left = None
                    node.inferred_right = None
            else:
                node.inferred_left = node.explicit_left
                node.inferred_right = node.explicit_right if node.explicit_right is not None else None

    for root in fragment_root_map.values():
        propagate(root)

    # 构建跨 Fragment 子节点引用（RemoteSource -> 目标 Fragment 根节点）
    for node in all_nodes:
        if node.name == 'RemoteSource' and node.remote_target_fragment is not None:
            target_frag = node.remote_target_fragment
            if target_frag in fragment_root_map:
                target_root = fragment_root_map[target_frag]
                if not hasattr(node, 'cross_children'):
                    node.cross_children = []
                node.cross_children.append(target_root)
                if debug:
                    print(f"RemoteSource {node.plan_id} -> target frag {target_frag} root {target_root.plan_id}")

    # 生成最终结果
    result = []
    for node in all_nodes:
        child_ids = [str(c.plan_id) for c in node.children]
        if hasattr(node, 'cross_children'):
            child_ids.extend(str(c.plan_id) for c in node.cross_children)
        # 去重保留顺序
        seen = set()
        unique_ids = []
        for cid in child_ids:
            if cid not in seen:
                seen.add(cid)
                unique_ids.append(cid)
        result.append({
            'operator': node.name,
            'plan_id': node.plan_id,
            'child_plan_ids': ','.join(unique_ids),
            'l_input_rows': node.inferred_left if node.inferred_left is not None else 0,
            'r_input_rows': node.inferred_right if node.inferred_right is not None else 0,
            'output_rows': node.inferred_output if node.inferred_output is not None else '',
            'cpu_time_ms': node.cpu_time_ms if node.cpu_time_ms is not None else ''
        })
    return result


def extract_query_id(text):
    if not text:
        return None
    pattern = re.compile(r'\b\d{8}_\d{6}_\d{5}_[a-z0-9]+\b')
    m = pattern.search(text)
    return m.group(0) if m else None


def normalize_server_url(server):
    if server.startswith('http://') or server.startswith('https://'):
        return server.rstrip('/')
    return f"http://{server.rstrip('/')}"


def fetch_query_info(server, query_id, timeout=30):
    base = normalize_server_url(server)
    url = f"{base}/v1/query/{query_id}"
    req = request.Request(url, headers={'Accept': 'application/json'})
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode('utf-8')
            return json.loads(payload)
    except error.HTTPError as e:
        raise RuntimeError(f"Failed to fetch query info from {url}: HTTP {e.code}")
    except error.URLError as e:
        raise RuntimeError(f"Failed to fetch query info from {url}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to parse query info JSON from {url}: {e}")


def parse_iso8601_utc(ts):
    if not ts or not isinstance(ts, str):
        return None
    # Presto/Trino 常见格式: 2026-04-24T08:10:43.123Z
    if ts.endswith('Z'):
        ts = ts[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def fetch_recent_query_id(server, expected_sql, timeout=30, lookback_seconds=600):
    """
    在无法从 presto-cli 输出中提取 query_id 时，兜底从 /v1/query 回查最近查询。
    """
    base = normalize_server_url(server)
    url = f"{base}/v1/query"
    req = request.Request(url, headers={'Accept': 'application/json'})
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode('utf-8')
            queries = json.loads(payload)
    except Exception:
        return None

    if not isinstance(queries, list):
        return None

    expect_norm = re.sub(r'\s+', ' ', expected_sql or '').strip().lower()
    now_utc = datetime.now(timezone.utc)
    best = None
    best_ts = None

    for q in queries:
        if not isinstance(q, dict):
            continue
        qid = q.get('queryId')
        if not qid:
            continue
        qtext = q.get('query', '')
        qtext_norm = re.sub(r'\s+', ' ', qtext).strip().lower()
        # 只匹配 explain analyze 的目标 SQL，避免误关联
        if expect_norm and expect_norm not in qtext_norm:
            continue

        # 优先完成态；没有 state 字段时也允许
        state = (q.get('state') or '').upper()
        if state and state not in ('FINISHED', 'RUNNING'):
            continue

        # 时间窗过滤
        created = parse_iso8601_utc(q.get('createTime') or q.get('executionStartTime'))
        if created is not None:
            age = (now_utc - created).total_seconds()
            if age > lookback_seconds:
                continue

        if best is None:
            best = qid
            best_ts = created
            continue

        if created is not None and (best_ts is None or created > best_ts):
            best = qid
            best_ts = created

    return best


def collect_operator_summaries(query_info):
    summaries = []

    def _collect_stage(stage):
        if not isinstance(stage, dict):
            return

        stage_stats = stage.get('stageStats', {})
        if isinstance(stage_stats, dict):
            ops = stage_stats.get('operatorSummaries', [])
            if isinstance(ops, list):
                for op in ops:
                    if isinstance(op, dict):
                        summaries.append(op)

        stage_stats_alt = stage.get('stats', {})
        if isinstance(stage_stats_alt, dict):
            ops = stage_stats_alt.get('operatorSummaries', [])
            if isinstance(ops, list):
                for op in ops:
                    if isinstance(op, dict):
                        summaries.append(op)

        for sub in stage.get('subStages', []) or []:
            _collect_stage(sub)

    _collect_stage(query_info.get('outputStage', {}))
    return summaries


def to_number(x, default=0.0):
    if x is None:
        return default
    try:
        return float(x)
    except Exception:
        return default


def safe_div(a, b):
    b = to_number(b, 0.0)
    if b == 0:
        return 0.0
    return to_number(a, 0.0) / b


def summarize_runtime_stats(runtime_stats_dict):
    if not isinstance(runtime_stats_dict, dict):
        return {
            'rt_stat_cnt': 0,
            'rt_driver_cpu_ns_sum': 0.0,
            'rt_queued_wall_ns_sum': 0.0,
            'rt_input_batches_sum': 0.0,
            'rt_output_batches_sum': 0.0,
            'rt_num_mem_alloc_sum': 0.0,
            'rt_hash_num_distinct_sum': 0.0,
            'rt_exchange_get_data_ns_sum': 0.0,
            'rt_scan_read_wall_ns_sum': 0.0
        }

    out = {
        'rt_stat_cnt': 0,
        'rt_driver_cpu_ns_sum': 0.0,
        'rt_queued_wall_ns_sum': 0.0,
        'rt_input_batches_sum': 0.0,
        'rt_output_batches_sum': 0.0,
        'rt_num_mem_alloc_sum': 0.0,
        'rt_hash_num_distinct_sum': 0.0,
        'rt_exchange_get_data_ns_sum': 0.0,
        'rt_scan_read_wall_ns_sum': 0.0
    }

    for k, v in runtime_stats_dict.items():
        if not isinstance(v, dict):
            continue
        out['rt_stat_cnt'] += 1
        k_lower = str(k).lower()
        sum_val = to_number(v.get('sum'), 0.0)
        if 'drivercputime' in k_lower:
            out['rt_driver_cpu_ns_sum'] += sum_val
        if 'queuedwall' in k_lower:
            out['rt_queued_wall_ns_sum'] += sum_val
        if 'inputbatches' in k_lower:
            out['rt_input_batches_sum'] += sum_val
        if 'outputbatches' in k_lower:
            out['rt_output_batches_sum'] += sum_val
        if 'nummemoryallocations' in k_lower:
            out['rt_num_mem_alloc_sum'] += sum_val
        if 'hashtable.numdistinct' in k_lower:
            out['rt_hash_num_distinct_sum'] += sum_val
        if 'prestoexchangesource.getdatananos' in k_lower:
            out['rt_exchange_get_data_ns_sum'] += sum_val
        if 'datasourcereadwallnanos' in k_lower:
            out['rt_scan_read_wall_ns_sum'] += sum_val

    return out


def enrich_with_runtime_stats(operators, query_info, debug=False):
    raw_summaries = collect_operator_summaries(query_info)
    runtime_stats = []

    qstats = query_info.get('queryStats', {}) if isinstance(query_info, dict) else {}
    query_context = {
        'query_total_tasks': qstats.get('totalTasks', ''),
        'query_total_drivers': qstats.get('totalDrivers', ''),
        'query_total_splits': qstats.get('totalSplits', ''),
        'query_peak_running_tasks': qstats.get('peakRunningTasks', ''),
        'query_total_cpu_time_ms': parse_duration_to_ms(qstats.get('totalCpuTime')),
        'query_total_scheduled_time_ms': parse_duration_to_ms(qstats.get('totalScheduledTime')),
        'query_total_blocked_time_ms': parse_duration_to_ms(qstats.get('totalBlockedTime')),
        'query_raw_input_positions': qstats.get('rawInputPositions', ''),
        'query_processed_input_positions': qstats.get('processedInputPositions', ''),
        'query_shuffled_positions': qstats.get('shuffledPositions', ''),
    }

    for summary in raw_summaries:
        operator_type = (
            summary.get('operatorType')
            or summary.get('operator')
            or summary.get('name')
            or ''
        )
        if not operator_type:
            continue

        rt_agg = summarize_runtime_stats(summary.get('runtimeStats'))

        row = {
            'operator_type': str(operator_type),
            'stage_id': summary.get('stageId', ''),
            'pipeline_id': summary.get('pipelineId', ''),
            'operator_id': summary.get('operatorId', ''),
            'plan_node_id': summary.get('planNodeId', ''),
            'dop': summary.get('totalDrivers', ''),
            'input_rows': summary.get('inputPositions', 0),
            'output_rows': summary.get('outputPositions', 0),
            'input_bytes': summary.get('inputDataSizeInBytes', 0),
            'output_bytes': summary.get('outputDataSizeInBytes', 0),
            'raw_input_rows': summary.get('rawInputPositions', 0),
            'raw_input_bytes': summary.get('rawInputDataSizeInBytes', 0),
            'spilled_bytes': summary.get('spilledDataSizeInBytes', 0),
            'peak_mem_bytes': summary.get('peakTotalMemoryReservationInBytes', 0),
            'user_mem_bytes': summary.get('userMemoryReservationInBytes', 0),
            'system_mem_bytes': summary.get('systemMemoryReservationInBytes', 0),
            'add_input_wall_ms': parse_duration_to_ms(summary.get('addInputWall')),
            'add_input_cpu_ms': parse_duration_to_ms(summary.get('addInputCpu')),
            'get_output_wall_ms': parse_duration_to_ms(summary.get('getOutputWall')),
            'get_output_cpu_ms': parse_duration_to_ms(summary.get('getOutputCpu')),
            'finish_wall_ms': parse_duration_to_ms(summary.get('finishWall')),
            'finish_cpu_ms': parse_duration_to_ms(summary.get('finishCpu')),
            'blocked_wall_ms': parse_duration_to_ms(summary.get('blockedWall')),
            'is_blocked_wall_ms': parse_duration_to_ms(summary.get('isBlockedWall')),
            'is_blocked_cpu_ms': parse_duration_to_ms(summary.get('isBlockedCpu')),
            'additional_cpu_ms': parse_duration_to_ms(summary.get('additionalCpu')),
            'rt_stat_cnt': rt_agg['rt_stat_cnt'],
            'rt_driver_cpu_ms': rt_agg['rt_driver_cpu_ns_sum'] / 1_000_000.0,
            'rt_queued_wall_ms': rt_agg['rt_queued_wall_ns_sum'] / 1_000_000.0,
            'rt_input_batches': rt_agg['rt_input_batches_sum'],
            'rt_output_batches': rt_agg['rt_output_batches_sum'],
            'rt_num_mem_alloc': rt_agg['rt_num_mem_alloc_sum'],
            'rt_hash_num_distinct': rt_agg['rt_hash_num_distinct_sum'],
            'rt_exchange_get_data_ms': rt_agg['rt_exchange_get_data_ns_sum'] / 1_000_000.0,
            'rt_scan_read_wall_ms': rt_agg['rt_scan_read_wall_ns_sum'] / 1_000_000.0,
        }

        total_wall_ms = (
            to_number(row['add_input_wall_ms'])
            + to_number(row['get_output_wall_ms'])
            + to_number(row['finish_wall_ms'])
        )
        row['total_wall_ms'] = total_wall_ms
        row['rows_per_driver_in'] = safe_div(row['input_rows'], row['dop'])
        row['rows_per_driver_out'] = safe_div(row['output_rows'], row['dop'])
        row['bytes_per_driver_in'] = safe_div(row['input_bytes'], row['dop'])
        row['cpu_ms_per_input_row'] = safe_div(row['rt_driver_cpu_ms'], row['input_rows'])
        row['wall_ms_per_input_row'] = safe_div(total_wall_ms, row['input_rows'])
        row['selectivity_rows'] = safe_div(row['output_rows'], row['input_rows'])
        row['selectivity_bytes'] = safe_div(row['output_bytes'], row['input_bytes'])
        row['blocked_ratio'] = safe_div(row['blocked_wall_ms'], total_wall_ms)
        row['mem_bytes_per_input_row'] = safe_div(row['peak_mem_bytes'], row['input_rows'])

        row.update(query_context)
        runtime_stats.append(row)

    max_dop = 0
    for item in runtime_stats:
        if isinstance(item.get('dop'), (int, float)) and item['dop'] > max_dop:
            max_dop = int(item['dop'])

    queues = {}
    for item in runtime_stats:
        key = item['operator_type'].lower()
        queues.setdefault(key, []).append(item)

    def _pick_runtime(op_name):
        target = op_name.lower()
        if target in queues and queues[target]:
            return queues[target].pop(0)
        for key in list(queues.keys()):
            if not queues[key]:
                continue
            if target in key or key in target:
                return queues[key].pop(0)
        return None

    for op in operators:
        matched = _pick_runtime(op['operator'])
        if matched:
            op.update(matched)
        else:
            op['dop'] = ''
            op['peak_mem_bytes'] = ''

    if debug:
        print(f"Runtime operator summaries: {len(runtime_stats)}, query_dop={max_dop}")

    return operators, max_dop


def run_presto_query(presto_jar, server, catalog, schema, sql):
    cmd = [
        'java', '-jar', presto_jar,
        '--server', server,
        '--catalog', catalog,
        '--schema', schema,
        '--execute', sql
    ]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=600)
        if result.returncode != 0:
            raise RuntimeError(f"presto-cli error (rc={result.returncode}): {result.stderr}")
        query_id = extract_query_id(result.stdout) or extract_query_id(result.stderr)
        return result.stdout, query_id
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Query timed out after 600s: {sql[:100]}")
    except Exception as e:
        raise RuntimeError(f"Failed to run presto-cli: {e}")


def main():
    parser = argparse.ArgumentParser(description='Extract operator left/right input rows from Presto EXPLAIN ANALYZE')
    parser.add_argument('--presto-jar', required=True, help='Path to presto-cli executable jar')
    parser.add_argument('--server', default='localhost:8082', help='Presto server address (host:port)')
    parser.add_argument('--catalog', default='hive', help='Catalog name')
    parser.add_argument('--schema', default='tpch_test', help='Schema name')
    parser.add_argument('--sql-file', required=True, help='File containing SQL queries (one per line)')
    parser.add_argument('--output', default='operator_rows.csv', help='Output CSV file')
    parser.add_argument('--debug', action='store_true', help='Print debug info')
    args = parser.parse_args()

    with open(args.sql_file, 'r') as f:
        sqls = [line.strip() for line in f if line.strip() and not line.startswith('--')]

    if not sqls:
        print("No SQL queries found.", file=sys.stderr)
        sys.exit(1)

    with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow([
            'query_id', 'plan_id', 'operator_type', 'child_plan',
            'l_input_rows', 'r_input_rows', 'output_rows', 'execution_time_ms',
            'query_dop',
            'stage_id', 'pipeline_id', 'operator_id', 'plan_node_id', 'dop',
            'input_rows', 'output_rows', 'input_bytes', 'output_bytes',
            'raw_input_rows', 'raw_input_bytes', 'spilled_bytes',
            'peak_mem_bytes', 'user_mem_bytes', 'system_mem_bytes',
            'add_input_wall_ms', 'add_input_cpu_ms', 'get_output_wall_ms', 'get_output_cpu_ms',
            'finish_wall_ms', 'finish_cpu_ms', 'blocked_wall_ms',
            'is_blocked_wall_ms', 'is_blocked_cpu_ms', 'additional_cpu_ms',
            'rt_stat_cnt', 'rt_driver_cpu_ms', 'rt_queued_wall_ms',
            'rt_input_batches', 'rt_output_batches', 'rt_num_mem_alloc',
            'rt_hash_num_distinct', 'rt_exchange_get_data_ms', 'rt_scan_read_wall_ms',
            'total_wall_ms', 'rows_per_driver_in', 'rows_per_driver_out', 'bytes_per_driver_in',
            'cpu_ms_per_input_row', 'wall_ms_per_input_row', 'selectivity_rows', 'selectivity_bytes',
            'blocked_ratio', 'mem_bytes_per_input_row',
            'query_total_tasks', 'query_total_drivers', 'query_total_splits', 'query_peak_running_tasks',
            'query_total_cpu_time_ms', 'query_total_scheduled_time_ms', 'query_total_blocked_time_ms',
            'query_raw_input_positions', 'query_processed_input_positions', 'query_shuffled_positions'
        ])

        for idx, sql in enumerate(sqls, start=1):
            print(f"Processing query {idx}/{len(sqls)}: {sql[:80]}...")
            explain_sql = f"EXPLAIN ANALYZE {sql}"
            try:
                output, query_id = run_presto_query(args.presto_jar, args.server, args.catalog, args.schema, explain_sql)
                operators = parse_operator_rows(output, debug=args.debug)
                query_dop = ''
                if not query_id:
                    query_id = fetch_recent_query_id(args.server, explain_sql)
                    if args.debug and query_id:
                        print(f"Recovered query_id from /v1/query: {query_id}")

                if query_id:
                    try:
                        query_info = fetch_query_info(args.server, query_id)
                        operators, query_dop = enrich_with_runtime_stats(operators, query_info, debug=args.debug)
                    except Exception as stat_err:
                        if args.debug:
                            print(f"Warning: failed to enrich runtime stats for {query_id}: {stat_err}")
                elif args.debug:
                    print("Warning: query_id not found in presto-cli output; skip runtime stats enrichment")

                for op in operators:
                    writer.writerow([
                        f"query_{idx}",
                        op.get('plan_id', ''),
                        op.get('operator', ''),
                        op.get('child_plan_ids', ''),
                        op.get('l_input_rows', ''),
                        op.get('r_input_rows', ''),
                        op.get('output_rows', ''),
                        op.get('cpu_time_ms', ''),
                        query_dop,
                        op.get('stage_id', ''), op.get('pipeline_id', ''), op.get('operator_id', ''), op.get('plan_node_id', ''), op.get('dop', ''),
                        op.get('input_rows', ''), op.get('output_rows', ''), op.get('input_bytes', ''), op.get('output_bytes', ''),
                        op.get('raw_input_rows', ''), op.get('raw_input_bytes', ''), op.get('spilled_bytes', ''),
                        op.get('peak_mem_bytes', ''), op.get('user_mem_bytes', ''), op.get('system_mem_bytes', ''),
                        op.get('add_input_wall_ms', ''), op.get('add_input_cpu_ms', ''), op.get('get_output_wall_ms', ''), op.get('get_output_cpu_ms', ''),
                        op.get('finish_wall_ms', ''), op.get('finish_cpu_ms', ''), op.get('blocked_wall_ms', ''),
                        op.get('is_blocked_wall_ms', ''), op.get('is_blocked_cpu_ms', ''), op.get('additional_cpu_ms', ''),
                        op.get('rt_stat_cnt', ''), op.get('rt_driver_cpu_ms', ''), op.get('rt_queued_wall_ms', ''),
                        op.get('rt_input_batches', ''), op.get('rt_output_batches', ''), op.get('rt_num_mem_alloc', ''),
                        op.get('rt_hash_num_distinct', ''), op.get('rt_exchange_get_data_ms', ''), op.get('rt_scan_read_wall_ms', ''),
                        op.get('total_wall_ms', ''), op.get('rows_per_driver_in', ''), op.get('rows_per_driver_out', ''), op.get('bytes_per_driver_in', ''),
                        op.get('cpu_ms_per_input_row', ''), op.get('wall_ms_per_input_row', ''), op.get('selectivity_rows', ''), op.get('selectivity_bytes', ''),
                        op.get('blocked_ratio', ''), op.get('mem_bytes_per_input_row', ''),
                        op.get('query_total_tasks', ''), op.get('query_total_drivers', ''), op.get('query_total_splits', ''), op.get('query_peak_running_tasks', ''),
                        op.get('query_total_cpu_time_ms', ''), op.get('query_total_scheduled_time_ms', ''), op.get('query_total_blocked_time_ms', ''),
                        op.get('query_raw_input_positions', ''), op.get('query_processed_input_positions', ''), op.get('query_shuffled_positions', '')
                    ])
            except Exception as e:
                print(f"Error on query {idx}: {e}", file=sys.stderr)
                writer.writerow([f"query_{idx}", "", f"ERROR: {str(e)}", "", 0, 0, '', '', '', ''])

    print(f"Results written to {args.output}")


if __name__ == '__main__':
    main()
