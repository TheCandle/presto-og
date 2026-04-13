#!/bin/bash

# 获取所有 SQL 文件
for file in *.sql; do
  # 给每个文件添加 "EXPLAIN ANALYZE"
  # 使用 sed 将 EXPLAIN ANALYZE 放在查询的开头，并保留注释
  # `1s/^/EXPLAIN ANALYZE\n/` 作用是把 EXPLAIN ANALYZE 放到每个文件的第一行
  sed -i '1s/^/EXPLAIN ANALYZE\n/' "$file"
done
