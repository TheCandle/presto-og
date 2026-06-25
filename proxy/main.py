import subprocess
import os
import sys
import time
import argparse
import csv
import json
import os
import sys
import time
from typing import List, Dict, Optional, Tuple, Any

import requests

# OG配置项
GAUSS_DATA_DIR = '/home/yjh/og-tpch-workspace/GaussData'
GAUSS_LOG_FILE = '/home/yjh/og-tpch-workspace/openGauss.log'
GAUSS_PORT = '40040'
GAUSS_PASSWORD = 'Asly0824@'
DB_USER = 'yjh'
DB_NAME = 'tpch_cs'
og_outfile = './output/og_outfile'
og_errfile = './output/og_errfile'
GSQL_CMD = f"gsql -p {GAUSS_PORT} -U {DB_USER} -d {DB_NAME} -q -X"
TIMEOUT = 60  # 设置超时时间为 60 秒

# presto配置项
DEFAULT_PRESTO_URL = "http://localhost:8082"
DEFAULT_CATALOG = "hive"
DEFAULT_SCHEMA = "tpch_test"
DEFAULT_SESSION_PARAMS = ["task_concurrency=64"]
DEFAULT_TIMEOUT = 1500
DEFAULT_DOP = 64
presto_jar_path = '/home/yjh/Project/presto-og/presto.jar'  # 替换为你本地的 presto.jar 文件路径
presto_outfile = './output/presto_outfile'
presto_errfile = './output/presto_errfile'



def log_warn(message):
    print(f"[WARN] {message}")

def log_info(message):
    print(f"[INFO] {message}")

def log_error(message):
    print(f"[ERROR] {message}")
    
def check_success(command, error_message):
    if command.returncode != 0:
        log_error(error_message)
        sys.exit(1)

def wait_for_db(port, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # 尝试连接数据库
            result = subprocess.run(
                ["gsql", "-p", GAUSS_PORT, "-U", DB_USER, "-d", DB_NAME, "-q", "-X", "-A", "-t", "-c", "SELECT 1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            if result.returncode == 0:
                log_info(f"数据库连接成功，端口 {port} 可用")
                return True
        except subprocess.CalledProcessError:
            pass
        log_warn(f"数据库未响应，等待 {timeout} 秒...")
        time.sleep(5)
    log_error("数据库启动超时，无法连接。")
    sys.exit(1)

def start_db(): 
  # 确保数据库已启动
  try:
      result = subprocess.run(
          ["gs_ctl", "status", "-D", GAUSS_DATA_DIR],
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE
      )
      if result.returncode != 0:
          log_warn("数据库未运行，尝试启动...")
          start_result = subprocess.run(
              ["gs_ctl", "start", "-D", GAUSS_DATA_DIR, "-Z", "single_node", "-l", GAUSS_LOG_FILE],
              stdout=subprocess.PIPE,
              stderr=subprocess.PIPE
          )
          check_success(start_result, "启动数据库失败")
  except Exception as e:
      log_error(f"执行 gs_ctl 命令失败: {str(e)}")
      sys.exit(1)
  
  # 等待数据库启动
  wait_for_db(GAUSS_PORT)
  
  # 设置数据库连接
  os.environ['PGPASSWORD'] = GAUSS_PASSWORD
  
  # 测试数据库连接
  log_info("测试数据库连接...")
  try:
      result = subprocess.run(
          ["gsql", "-p", GAUSS_PORT, "-U", DB_USER, "-d", DB_NAME, "-q", "-X", "-A", "-t", "-c", "SELECT 1"],
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE
      )
      if result.returncode != 0:
          log_error("无法连接到数据库，请检查配置或数据库状态。")
          sys.exit(1)
  except subprocess.CalledProcessError:
      log_error("无法连接到数据库，请检查配置或数据库状态。")
      sys.exit(1)
  
  log_info("数据库连接成功！")

	
def og_execute_sql(sql, out_file, err_file):
    try:
        command = ["timeout", str(TIMEOUT), "bash", "-c", f"echo \"{sql}\" | {GSQL_CMD}"]
        print("TP负载")
        print("调用命令:", " ".join(command))
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        output = result.stdout.decode()
        error_output = result.stderr.decode()
        if output:
            print(output, end="")
        if error_output:
            print(error_output, end="", file=sys.stderr)
        return result.returncode, output, error_output
    except subprocess.CalledProcessError as e:
        log_error(f"执行 SQL 时出现错误: {str(e)}")
        return e.returncode, "", str(e)

def presto_execute_sql(sql, out_file, err_file):
    try:
        command = [
            'java', 
            '-jar', presto_jar_path,
            '--server', 'localhost:8082',
            '--catalog', 'hive',
            '--schema', 'tpch_test',
            '--execute', sql
        ]
        print("AP负载")
        print("调用命令:", " ".join(command))
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        output = result.stdout
        error_output = result.stderr
        if output:
            print(output, end="")
        if error_output:
            print(error_output, end="", file=sys.stderr)
        return result.returncode, output, error_output
    except subprocess.CalledProcessError as e:
        log_error(f"执行 SQL 时出现错误: {str(e)}")
        return e.returncode, "", str(e)

def query_is_ap(query):
    query_lower = query.lower()
    ap_keywords = ["sum", "avg", "min", "max", "count", "join", "group by"]
    return any(keyword in query_lower for keyword in ap_keywords)

def query_is_tp(query):
    if("tp" in query):
        return True
    return False


def send_query_to_db(query):
    if(query_is_ap(query)):
        presto_execute_sql(query, presto_outfile, presto_errfile)
    else:
        og_execute_sql(query, og_outfile, og_errfile)

# 主程序
def main():
    start_db()
    query = input("请输入查询内容: ")
    send_query_to_db(query)

if __name__ == "__main__":
    main()
