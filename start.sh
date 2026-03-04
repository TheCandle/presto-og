# docker-compose run --rm presto-cli presto --server http://coordinator:8082 --catalog hive --schema default
./presto.jar --server localhost:8082 --catalog hive --schema tpch_test
