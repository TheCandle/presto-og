run_stage_pipeline_dop:
	python3 /home/yjh/Project/presto-og/tpch-test/tpch_presto_subtract_stage_pipeline_dop.py \
	--presto-url http://localhost:8082 \
	--catalog hive \
	--schema tpch_test \
	--query-dir /home/yjh/Project/presto-og/tpch-queries/test-query \
	--pipeline-dop-file /home/yjh/Project/presto-og/tpch-test/stage_threadblock_optimal_dop.csv

run_stage_pipeline_dop_until_success:
	python3 /home/yjh/Project/presto-og/tpch-test/run_stage_pipeline_dop_until_success.py \
	--presto-url http://localhost:8082 \
	--catalog hive \
	--schema tpch_test \
	--query-dir /home/yjh/Project/presto-og/tpch-queries/test-query \
	--pipeline-dop-file /home/yjh/Project/presto-og/tpch-test/stage_threadblock_optimal_dop.csv \
	--max-rounds 10