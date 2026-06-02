
# docker-compose run --rm presto-cli presto --server http://coordinator:8082 --catalog hive --schema default
# 从宿主机复制到容器
docker cp recover_external_tables.sql presto-og-hive-server-1:/tmp/

# 在 Hive 容器中执行
docker exec presto-og-hive-server-1 hive -f /tmp/recover_external_tables.sql
./presto.jar --server localhost:8082 --catalog hive --schema tpch_test --file create_revenue0.sql
