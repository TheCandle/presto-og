#!/bin/bash
export HADOOP_HOME=/opt/hadoop-2.7.4
echo "Setting CLASSPATH from Hadoop (expanded)..."
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath | tr ':' '\n' | while read entry; do
  if [[ "$entry" == *"*" ]]; then
    dir=${entry%/*}
    for jar in $dir/*.jar; do
      echo -n "$jar:"
    done
  else
    echo -n "$entry:"
  fi
done | sed 's/:$//')
if [ $? -ne 0 ] || [ -z "$CLASSPATH" ]; then
    echo "Failed to set CLASSPATH, exiting."
    exit 1
fi
echo "CLASSPATH set successfully (length: ${#CLASSPATH})"

# 新打包的worker版本把watchdog删除了，所以下面是之前的入口粘贴过来了，删除了watchdog启动的部分
echo "SERVER_HOST=$SERVER_HOST"
echo "SERVER_PORT=$SERVER_PORT"
echo "WORKER_PORT=$WORKER_PORT"

sed -i "s|http://<replace_host>:<replace_port>|http://$SERVER_HOST:$SERVER_PORT|" /opt/presto-server/etc/config.properties
sed -i "s|http-server.http.port=7777|http-server.http.port=$WORKER_PORT|" /opt/presto-server/etc/config.properties

GLOG_logtostderr=1 presto_server --etc-dir=/opt/presto-server/etc
# 调用原始 entrypoint 脚本
# exec /opt/entrypoint.sh "$@"
