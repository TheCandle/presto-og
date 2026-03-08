#!/bin/bash
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "
CREATE TABLE IF NOT EXISTS pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
"

