-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS tpch_test;
USE tpch_test;

-- region 表
CREATE EXTERNAL TABLE region (
    r_regionkey INT,
    r_name STRING,
    r_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/region';

-- nation 表
CREATE EXTERNAL TABLE nation (
    n_nationkey INT,
    n_name STRING,
    n_regionkey INT,
    n_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/nation';

-- customer 表
CREATE EXTERNAL TABLE customer (
    c_custkey BIGINT,
    c_name STRING,
    c_address STRING,
    c_nationkey INT,
    c_phone STRING,
    c_acctbal DOUBLE,
    c_mktsegment STRING,
    c_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/customer';

-- supplier 表
CREATE EXTERNAL TABLE supplier (
    s_suppkey BIGINT,
    s_name STRING,
    s_address STRING,
    s_nationkey INT,
    s_phone STRING,
    s_acctbal DOUBLE,
    s_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/supplier';

-- part 表
CREATE EXTERNAL TABLE part (
    p_partkey BIGINT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/part';

-- partsupp 表
CREATE EXTERNAL TABLE partsupp (
    ps_partkey BIGINT,
    ps_suppkey BIGINT,
    ps_availqty INT,
    ps_supplycost DOUBLE,
    ps_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/partsupp';

-- orders 表
CREATE EXTERNAL TABLE orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus STRING,
    o_totalprice DOUBLE,
    o_orderdate DATE,
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT,
    o_comment STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/orders';

-- lineitem 表
CREATE EXTERNAL TABLE lineitem (
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
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/tpch_test.db/lineitem';
