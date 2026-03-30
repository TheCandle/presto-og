## 启动容器
docker compose up -d

**注意**：docker-compose.yml中hive-server volumes会挂载tpch的数据目录，需要根据自己的路径修改一下。

## 导入数据
等容器启动完成后。

运行
    ./load_tpch_to_hive.sh
构建tpch表结构并将数据导入到hive中。

**注意**：load_tpch_to_hive.sh也需要修改数据目录路径为自己的实际路径

## 测试

下载presto-cli：


  wget https://repo1.maven.org/maven2/io/prestosql/presto-cli/308/presto-cli-308-executable.jar

  mv presto-cli-308-executable.jar presto.jar
  
  chmod +x presto.jar

运行./presto.jar --server localhost:8082 --catalog hive --schema tpch_test




**注意**：只有第一次启动需要导入数据，后面这个数据会持久化到卷中。但是表结构没有持久化，后续如果重新启动的话运行

  bash start.sh

重新创建表结构
