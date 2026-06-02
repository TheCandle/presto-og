current_branch := $(shell git rev-parse --abbrev-ref HEAD)
test: 
	python3 tpch-test/tpch_presto_subtract.py \
        --query-dir ./tpch-queries/explaintpch \
        --generate-query-count 72 \
        --dbgen-dir ./tpch-dbgen \
        --scale-factor 100 \
        --overwrite-generated \
        --dop-list 2,4,8,16,24,32,40 \
        --warmup 2 \
        --runs 3
build:
	docker build -t bde2020/hive:$(current_branch) ./

run_query:
	python3 tpch-test/tpch_presto_subtract.py \
          --query-dir ./tpch-queries/test-query \
          --generate-query-count 22 \
          --dbgen-dir ./tpch-dbgen \
          --scale-factor 100 \
          --overwrite-generated \
          --dop-list 16 \
          --warmup 2 \
	  --runs 3
