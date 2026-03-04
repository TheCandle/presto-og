#!/bin/bash
# =====================================================
# TPC-H 数据加载脚本 (Hive + Parquet)
# 用途：将本地 TPC-H 数据复制到 Hive 容器，
#       创建文本表并加载数据，然后转换为 Parquet 格式。
# 用法：./load_tpch_to_hive.sh [--force]
#   --force: 强制重新创建所有表并重新加载数据
# =====================================================

set -e  # 遇到错误立即退出

# -------------------- 配置参数 --------------------
HIVE_CONTAINER="docker-hive-hive-server-1"      # Hive 容器名
LOCAL_DATA_DIR="/home/candle/Project/pg/tpchdata01"  # 宿主机数据目录
CONTAINER_DATA_DIR="/opt/tpch-data"             # 容器内临时数据目录
HIVE_DB="tpch_test"                              # Hive 数据库名
KEEP_TEXT_TABLES=false                           # 是否保留文本表 (true/false)
# -------------------------------------------------

# 检查本地数据目录是否存在
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "错误：本地数据目录 $LOCAL_DATA_DIR 不存在！"
    exit 1
fi

# 处理命令行参数
FORCE_RECREATE=false
if [ "$1" == "--force" ]; then
    FORCE_RECREATE=true
    echo "强制重建模式：将删除所有现有表并重新加载数据。"
fi

echo "========================================="
echo "开始 TPC-H 数据加载流程"
echo "Hive 容器: $HIVE_CONTAINER"
echo "本地数据目录: $LOCAL_DATA_DIR"
echo "目标数据库: $HIVE_DB"
echo "========================================="

# 1. 在容器内创建数据目录
echo "[1/7] 在容器内创建数据目录..."
docker exec $HIVE_CONTAINER mkdir -p $CONTAINER_DATA_DIR

# 2. 复制数据文件到容器
echo "[2/7] 复制数据文件到容器..."
docker cp $LOCAL_DATA_DIR/. $HIVE_CONTAINER:$CONTAINER_DATA_DIR/
echo "文件复制完成。"

# 3. 验证文件列表
echo "[3/7] 验证文件列表..."
docker exec $HIVE_CONTAINER ls -l $CONTAINER_DATA_DIR/

# 4. 构建 Hive 初始化 SQL
echo "[4/7] 准备 Hive 执行脚本..."

# 生成临时 SQL 文件
TMP_SQL="/tmp/hive_load_$$.sql"
cat > $TMP_SQL <<EOF
-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS $HIVE_DB;
USE $HIVE_DB;

-- 设置执行引擎（可选，根据你的环境调整）
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

-- 创建文本表（带 _text 后缀）
CREATE TABLE IF NOT EXISTS region_text (
    r_regionkey INT,
    r_name STRING,
    r_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS nation_text (
    n_nationkey INT,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS customer_text (
    c_custkey BIGINT,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DOUBLE,
    c_mktsegment STRING,
    c_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS supplier_text (
    s_suppkey BIGINT,
    s_name STRING,
    s_address STRING,
    s_nationkey INT,
    s_phone STRING,
    s_acctbal DOUBLE,
    s_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS part_text (
    p_partkey BIGINT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS partsupp_text (
    ps_partkey BIGINT,
    ps_suppkey BIGINT,
    ps_availqty INT,
    ps_supplycost DOUBLE,
    ps_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS orders_text (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus STRING,
    o_totalprice DOUBLE,
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS lineitem_text (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber INT,
    l_quantity DOUBLE,
    l_extendedprice DOUBLE,
    l_discount DOUBLE,
    l_tax DOUBLE,
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

-- 加载数据到文本表（使用 OVERWRITE，每次都会重新加载）
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/region.tbl' OVERWRITE INTO TABLE region_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/nation.tbl' OVERWRITE INTO TABLE nation_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/customer.tbl' OVERWRITE INTO TABLE customer_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/supplier.tbl' OVERWRITE INTO TABLE supplier_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/part.tbl' OVERWRITE INTO TABLE part_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/partsupp.tbl' OVERWRITE INTO TABLE partsupp_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/orders.tbl' OVERWRITE INTO TABLE orders_text;
LOAD DATA LOCAL INPATH '$CONTAINER_DATA_DIR/lineitem.tbl' OVERWRITE INTO TABLE lineitem_text;
EOF

# 如果强制重建，则添加删除 Parquet 表的语句
if [ "$FORCE_RECREATE" = true ]; then
    cat >> $TMP_SQL <<EOF
-- 强制重建：删除现有 Parquet 表（如果存在）
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;
EOF
fi

echo "临时 SQL 文件已生成: $TMP_SQL"

# 5. 将临时 SQL 文件复制到容器并执行
echo "[5/7] 执行 Hive SQL（创建文本表并加载数据）..."
docker cp $TMP_SQL $HIVE_CONTAINER:/tmp/hive_load.sql
docker exec $HIVE_CONTAINER hive -f /tmp/hive_load.sql
rm $TMP_SQL

# 6. 创建 Parquet 表（逐表处理）
echo "[6/7] 开始创建 Parquet 表（标准名称）..."

TABLES=("region" "nation" "customer" "supplier" "part" "partsupp" "orders" "lineitem")

for TABLE in "${TABLES[@]}"; do
    echo "处理表: $TABLE"
    
    # 检查 Parquet 表是否已存在且有数据
    COUNT=$(docker exec $HIVE_CONTAINER hive -S -e "SELECT COUNT(*) FROM $HIVE_DB.$TABLE;" 2>/dev/null | tr -d ' ' | grep -E '^[0-9]+$' || echo "0")
    
    if [ "$COUNT" -gt 0 ] && [ "$FORCE_RECREATE" = false ]; then
        echo "  表 $TABLE 已存在且有 $COUNT 行数据，跳过转换。"
    else
        echo "  创建 Parquet 表 $TABLE 并插入数据..."
        # 如果强制重建，表可能已被删除，这里直接创建即可（如果表已存在，下面的语句会失败，但我们在 FORCE_RECREATE 时已删除，所以安全）
        # 使用 CREATE TABLE ... AS SELECT ... 如果表已存在会报错，所以我们先尝试删除（但只在 FORCE_RECREATE 时删除过，普通模式下如果表存在但行数为0，也会进入这里，但表存在会导致 CREATE 失败）
        # 因此，更稳健的做法是：如果表存在，先删除再创建（普通模式下，表存在但行数为0的情况较少，但以防万一）
        if [ "$COUNT" -eq 0 ] && [ "$FORCE_RECREATE" = false ]; then
            # 表存在但无数据（或表不存在），先尝试删除（忽略错误）
            docker exec $HIVE_CONTAINER hive -e "DROP TABLE IF EXISTS $HIVE_DB.$TABLE;" >/dev/null 2>&1 || true
        fi
        # 创建 Parquet 表并从文本表插入
        docker exec $HIVE_CONTAINER hive -e "USE $HIVE_DB; CREATE TABLE $TABLE STORED AS PARQUET AS SELECT * FROM ${TABLE}_text;"
        echo "  表 $TABLE 创建完成。"
    fi
done

# 7. 清理：删除文本表（可选）
if [ "$KEEP_TEXT_TABLES" = false ]; then
    echo "[7/7] 删除文本表（释放空间）..."
    for TABLE in "${TABLES[@]}"; do
        docker exec $HIVE_CONTAINER hive -e "DROP TABLE IF EXISTS $HIVE_DB.${TABLE}_text;"
    done
    echo "文本表已删除。"
else
    echo "[7/7] 保留文本表（根据配置）"
fi

echo "========================================="
echo "TPC-H 数据加载完成！"
echo "现在可以在 Presto 中查询："
echo "  USE hive.$HIVE_DB;"
echo "  SELECT COUNT(*) FROM lineitem;"
echo "========================================="
